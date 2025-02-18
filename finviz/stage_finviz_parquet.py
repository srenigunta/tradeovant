import os
import glob
import logging
import shutil
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from tradeovant.imports.common_utils import LocalS3WithDirectory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_column_mapping(mapping_file):
    """
    Load the column mapping CSV into a dict: {current_col: new_col}.
    If you do not need column renaming for some dataset, pass an empty CSV or skip it.
    """
    try:
        df = pd.read_csv(mapping_file)  # columns: current_column, new_column
        mapping_dict = df.set_index('current_column')['new_column'].to_dict()
        logging.info(f"Loaded column mapping from {mapping_file}")
        return mapping_dict
    except Exception as e:
        logging.error(f"Failed to load column mapping {mapping_file}: {e}")
        raise

def list_csv_files_in_partition(raw_dir):
    """
    List all CSV files in raw_dir.
    If your raw data is stored in partition-based subfolders, adjust logic accordingly.
    """
    pattern = os.path.join(raw_dir, "*.csv")
    return glob.glob(pattern)

def parse_partition_from_filename(filepath):
    """
    Extract file_version_date from the filename or subfolder if needed.
    For example, if your file is named 'fundamentals_20250101.csv'
    you can parse '20250101' from it. Adjust as required.
    """
    base = os.path.basename(filepath)
    # E.g. 'fundamentals_20250101.csv' -> '20250101'
    # This is just an example pattern. Customize to your real file naming.
    parts = base.split("_")
    if len(parts) >= 2:
        return parts[-1].replace(".csv", "")  # e.g. '20250101'
    else:
        return None

def get_existing_partitions(stage_dir, partition_column="file_version_date"):
    """
    Return a set of existing partition values from the stage directory
    by looking for subfolders like file_version_date=YYYYMMDD.
    """
    if not os.path.exists(stage_dir):
        return set()
    partition_values = set()
    for name in os.listdir(stage_dir):
        if name.startswith(f"{partition_column}="):
            val = name.split("=")[1]
            partition_values.add(val)
    return partition_values

def duckdb_read_csv_as_strings(csv_file, has_header=True):
    """
    Read the CSV file in DuckDB, forcing EVERY column to VARCHAR
    and replacing any NULL with an empty string ('').
    Returns a PyArrow Table with purely string columns.
    """
    import duckdb

    # 1) Connect & read the header row only (LIMIT 0) to discover column names
    conn = duckdb.connect()
    # Create a temp table with no data, just the schema
    conn.execute(f"""
        CREATE TEMP TABLE tmpcsv AS
        SELECT * FROM read_csv_auto('{csv_file}', HEADER={str(has_header).upper()}) LIMIT 0
    """)
    # Inspect the columns
    col_info = conn.execute("PRAGMA table_info('tmpcsv')").fetchall()
    # col_info rows: (cid, name, type, notnull, dflt_value, pk)
    duckdb_columns = [f'"{row[1]}"' for row in col_info]  # e.g. ['col1','col2',...]

    # 2) Build a SELECT statement that forces each column to string
    #    and coalesces nulls to empty string
    select_parts = []
    for col in duckdb_columns:
        # COALESCE(CAST(col AS VARCHAR), '') as col
        select_parts.append(f"COALESCE(CAST({col} AS VARCHAR), '') AS {col}")

    select_clause = ",\n  ".join(select_parts)
    query = f"""
    SELECT
      {select_clause}
    FROM read_csv_auto('{csv_file}', HEADER={str(has_header).upper()})
    """
    # 3) Execute the query & fetch as a PyArrow Table
    arrow_table = conn.execute(query).arrow()
    conn.close()
    return arrow_table

def rename_columns(arrow_table, column_mapping):
    """
    Rename columns in a PyArrow table using the mapping {old_col: new_col}.
    """
    # Build new field list with updated names
    new_fields = []
    for field in arrow_table.schema:
        if field.name in column_mapping:
            new_fields.append(pa.field(column_mapping[field.name], field.type, field.nullable))
        else:
            new_fields.append(field)

    # Reconstruct schema with renamed fields
    new_schema = pa.schema(new_fields)
    # Create a list of arrays with the same order as new_fields
    arrays = []
    for field in arrow_table.schema:
        col_data = arrow_table.column(field.name)
        # If field.name is in column_mapping, we place it in the corresponding new name slot
        if field.name in column_mapping:
            arrays.append(col_data)
        else:
            arrays.append(col_data)

    # Build new table
    renamed_table = pa.Table.from_arrays(arrays, schema=new_schema)
    return renamed_table

def add_partition_column(table, partition_val, partition_column="file_version_date"):
    """
    Append a column to the Arrow table representing the partition value.
    E.g., file_version_date = '20250101'
    """
    num_rows = table.num_rows
    arr = pa.array([partition_val] * num_rows, type=pa.string())
    return table.append_column(partition_column, arr)

def process_csv_files(raw_dir,
                      stage_dir,
                      column_mapping,
                      partition_column="file_version_date",
                      filename=None,
                      rerun_dates=None):
    """
    For each CSV in raw_dir:
    - Parse partition from filename
    - Skip if partition exists unless in rerun_dates
    - Read with duckdb_read_csv_as_strings
    - Rename columns (optional)
    - Add partition column
    - Write as partitioned Parquet
    """
    existing_parts = get_existing_partitions(stage_dir, partition_column)
    logging.info(f"Existing partitions in stage: {len(existing_parts)}")

    csv_files = list_csv_files_in_partition(raw_dir)
    logging.info(f"Found {len(csv_files)} CSV files in {raw_dir}.")

    for csv_file in csv_files:
        partition_val = parse_partition_from_filename(csv_file)
        if not partition_val:
            continue

        # Decide skip or re-run
        if rerun_dates and partition_val in rerun_dates:
            do_overwrite = True
        elif partition_val in existing_parts:
            continue
        else:
            do_overwrite = False

        # 1) Read CSV with all columns as strings, coalesce null -> ""
        arrow_table = duckdb_read_csv_as_strings(csv_file, has_header=True)

        # 2) Rename columns using your custom logic
        arrow_table = rename_columns(arrow_table, column_mapping)

        # 3) Add partition column
        arrow_table = add_partition_column(arrow_table, partition_val, partition_column)

        partition_dir = os.path.join(stage_dir, rf"{partition_column}={partition_val}")
        if os.path.exists(partition_dir):
            shutil.rmtree(partition_dir)
            logging.info(f"Removed existing partition dir: {partition_dir}")
        else:
            os.makedirs(partition_dir, exist_ok=True)
        out_file = os.path.join(stage_dir, rf"{partition_column}={partition_val}", f"{filename}.parquet")

        # 4) Write partitioned Parquet
        pq.write_table(
            arrow_table,
            out_file,
            compression="snappy"
        )
        logging.info(f"Wrote Parquet data to {stage_dir} for {partition_val}")

def main():
    # 1) Initialize local storage
    storage = LocalS3WithDirectory()
    logging.info("Initialized local storage.")

    # 2) Define raw/stage paths and a column mapping file
    fd_raw_path = os.path.join(storage.BASE_PATH, "raw_store", "finviz", "fundamentals")
    fd_stage_path = os.path.join(storage.BASE_PATH, "stage_store", "finviz", "fundamentals")
    fd_mapping_file = r"D:\CodeBase\tradeovant\config\finviz_fd_mapping.csv"

    # 3) Load the fundamentals mapping
    try:
        fd_mapping = load_column_mapping(fd_mapping_file)
    except Exception as e:
        logging.error(f"Failed to load column mapping: {e}")
        fd_mapping = {}  # Fallback: no renaming

    # 4) Example: process fundamentals CSV
    #    Let's say we want to rerun partitions ["20250101"] if needed
    rerun_dates = None  # or e.g. {"20250101"}
    logging.info("Processing fundamentals data.")
    process_csv_files(
        raw_dir=fd_raw_path,
        stage_dir=fd_stage_path,
        column_mapping=fd_mapping,
        partition_column="file_version_date",
        filename="fundamentals",
        rerun_dates=rerun_dates
    )
    logging.info("Fundamentals data processing complete.")

    # 5) Similarly for ratings
    rd_raw_path = os.path.join(storage.BASE_PATH, "raw_store", "finviz", "ratings")
    rd_stage_path = os.path.join(storage.BASE_PATH, "stage_store", "finviz", "ratings")
    rd_mapping_file = r"D:\CodeBase\tradeovant\config\finviz_rd_mapping.csv"

    # 6) Load the ratings mapping
    try:
        rd_mapping = load_column_mapping(rd_mapping_file)
    except Exception as e:
        logging.error(f"Failed to load column mapping: {e}")
        rd_mapping = {}  # Fallback: no renaming

    logging.info("Processing ratings data.")
    process_csv_files(
        raw_dir=rd_raw_path,
        stage_dir=rd_stage_path,
        column_mapping=rd_mapping,
        partition_column="file_version_date",
        filename="ratings",
        rerun_dates=None
    )
    logging.info("Ratings data processing complete.")

    # 6) Stop storage
    storage.stop()
    logging.info("All done.")

if __name__ == "__main__":
    main()
