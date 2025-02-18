import logging
import os
import glob
import shutil
import yaml
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime
from tradeovant.imports.common_utils import LocalS3WithDirectory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ------------------------------------------------------------------------------
# 1. YAML SCHEMA LOADING + ARROW SCHEMA BUILDING
# ------------------------------------------------------------------------------
def load_schemas(schema_file):
    """
    Load schema definitions from a YAML (or JSON) file.
    Expected format:
      schemas:
        screener: [{name: 'atr', type: 'string', nullable: True}, ...]
        trending: [...]
    Returns a dictionary, e.g.:
    {
      "screener": [{...}, {...}],
      "trending": [{...}, {...}]
    }
    """
    with open(schema_file, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return data["schemas"]

def build_pyarrow_schema(fields_def):
    """
    Convert a list of {name, type, nullable} dicts into a PyArrow schema.
    """
    arrow_fields = []
    for field_def in fields_def:
        field_name = field_def['name']
        field_type_str = field_def['type'].lower()
        nullable = field_def.get('nullable', True)

        # Basic mapping. Extend if you have more types than just string.
        if field_type_str == 'string':
            arrow_type = pa.string()
        else:
            arrow_type = pa.string()  # fallback for now

        arrow_fields.append(pa.field(field_name, arrow_type, nullable=nullable))

    return pa.schema(arrow_fields)


# ------------------------------------------------------------------------------
# 2. HELPERS FOR PARTITIONS AND FILENAME LOGIC
# ------------------------------------------------------------------------------
def parse_date_from_filename(filename):
    """
    Assuming filenames like 'screener_data_YYYYMMDD.json' or 'trending_data_YYYYMMDD.json',
    extract the date portion, e.g. '20250108'.
    """
    base = os.path.basename(filename)
    # e.g., 'screener_data_20250108.json'
    parts = base.split("_")  # ["screener", "data", "20250108.json"]
    if len(parts) >= 3:
        date_part = parts[2].replace('.json', '')  # "20250108"
        return date_part
    else:
        logging.warning(f"Could not parse date from filename: {filename}")
        return None

def get_processed_dates(stage_dir):
    """
    Return a set of date strings found in 'file_version_date=YYYYMMDD' subdirectories.
    """
    if not os.path.exists(stage_dir):
        return set()

    processed = set()
    for name in os.listdir(stage_dir):
        if name.startswith("file_version_date="):
            date_val = name.split("=")[1]
            processed.add(date_val)
    return processed


# ------------------------------------------------------------------------------
# 3. DUCKDB-BASED JSON READING WITH DEFAULTS
# ------------------------------------------------------------------------------
def duckdb_read_json_with_defaults(json_file, arrow_schema):
    """
    1. Identify which columns appear in the JSON (DuckDB inference).
    2. For each column in arrow_schema, generate COALESCE(col, '') if it exists,
       else '' as col if it's truly missing.
    3. Convert the DuckDB result to a PyArrow table.
    4. Reorder & cast to match arrow_schema.
    """

    # 1) Extract final column names from arrow_schema
    final_cols = [field.name for field in arrow_schema]

    # 2) Connect to DuckDB, inspect JSON columns
    conn = duckdb.connect()
    # We'll create a temporary table from the JSON with LIMIT 0 to see inferred columns
    conn.execute(f"CREATE TEMPORARY TABLE tmpjson AS SELECT * FROM read_json_auto('{json_file}') LIMIT 0;")
    # Now fetch the column names from 'tmpjson'
    duckdb_info = conn.execute("PRAGMA table_info('tmpjson')").fetchall()
    # Format: each row is (cid, name, type, notnull, dflt_value, pk)
    json_cols_in_duckdb = {row[1] for row in duckdb_info}  # e.g. {"atr", "symbol", ...}

    # 3) Build a SELECT statement that coalesces or returns '' for missing
    select_parts = []
    for col in final_cols:
        if col in json_cols_in_duckdb:
            # e.g. COALESCE(atr, '') AS atr
            select_parts.append(f"COALESCE(CAST({col} AS VARCHAR), '') AS {col}")
        else:
            # e.g. '' AS atr
            select_parts.append(f"'' AS {col}")

    select_clause = ",\n  ".join(select_parts)
    query = f"""
    SELECT
      {select_clause}
    FROM read_json_auto('{json_file}')
    """

    logging.debug(f"DuckDB query:\n{query}")

    # 4) Execute the query, fetch as Arrow table
    arrow_table = conn.execute(query).arrow()
    conn.close()

    # 5) Reorder columns in arrow_table to match arrow_schema EXACTLY
    # (DuckDB might produce them in the order we selected, but let's be safe)
    arrays_in_schema_order = []
    for field in arrow_schema:
        col_index = arrow_table.schema.get_field_index(field.name)
        arrays_in_schema_order.append(arrow_table.column(col_index))

    reordered_table = pa.Table.from_arrays(
        arrays_in_schema_order,
        schema=arrow_schema
    )

    # 6) Cast if needed (likely redundant if everything is string)
    table = reordered_table.cast(arrow_schema, safe=False)
    return table


# ------------------------------------------------------------------------------
# 4. ADD EXTRA COLUMNS + WRITE PARTITIONED PARQUET
# ------------------------------------------------------------------------------
def add_extra_columns(table, date_val):
    """
    Adds an 'autonum' and 'file_version_date' column to the Table.
    """
    num_rows = table.num_rows
    # autonum
    autonum_array = pa.array(range(num_rows), type=pa.int64())
    table = table.append_column("autonum", autonum_array)

    # file_version_date
    fv_array = pa.array([date_val] * num_rows, type=pa.string())
    table = table.append_column("file_version_date", fv_array)

    return table

def write_parquet_partitioned(table, stage_dir, overwrite=False):
    """
    Write the table to a partitioned Parquet dataset (by file_version_date).
    If overwrite=True, remove that partition folder first.
    """
    # Identify unique date partitions
    date_vals = table.column(table.schema.get_field_index("file_version_date")).unique().to_pylist()

    # If there's only one date in this table, we can remove that partition to 'overwrite'
    if overwrite and len(date_vals) == 1:
        part_dir = os.path.join(stage_dir, f"file_version_date={date_vals[0]}")
        if os.path.exists(part_dir):
            shutil.rmtree(part_dir)
            logging.info(f"Removed existing partition dir: {part_dir}")

    pq.write_to_dataset(
        table,
        root_path=stage_dir,
        partition_cols=["file_version_date"],
        compression="snappy",
        use_dictionary=True
    )
    logging.info(f"Data written to Parquet under: {stage_dir}")


# ------------------------------------------------------------------------------
# 5. MAIN PROCESS FUNCTION FOR EACH DATA TYPE
# ------------------------------------------------------------------------------
def process_data_type(data_name, schema_name, schema_dict, rerun_date=None):
    """
    data_name: e.g. "screener_data" or "trending_data"
    schema_name: e.g. "screener" or "trending" (dict key from schemas.yaml)
    rerun_date: optionally reprocess a specific date string (like "20250108").

    This function:
      - loads all 'data_name_*.json' from the raw directory
      - determines which dates are processed in stage
      - reprocesses 'rerun_date' or unprocessed dates
      - writes partitioned Parquet
    """
    # 1) Build raw/stage paths
    storage = LocalS3WithDirectory()
    raw_dir = os.path.join(storage.BASE_PATH, "raw_store", "stockinvest_us", data_name)
    stage_dir = os.path.join(storage.BASE_PATH, "stage_store", "stockinvest_us", data_name)

    # 2) Build a PyArrow schema for this data_name
    fields_def = schema_dict[schema_name]  # e.g. schema_dict["screener"]
    arrow_schema = build_pyarrow_schema(fields_def)

    # 3) List JSON files
    pattern = os.path.join(raw_dir, f"{data_name}_*.json")
    json_files = glob.glob(pattern)
    if not json_files:
        logging.info(f"No files found for pattern: {pattern}")
        return

    # 4) Identify processed dates
    processed = get_processed_dates(stage_dir)
    logging.info(f"Existing partitions for {data_name}: {len(processed)}")

    # 5) Decide which files to process
    to_process = []
    for f in json_files:
        date_val = parse_date_from_filename(f)
        if not date_val:
            continue

        if rerun_date and date_val == rerun_date:
            to_process.append(f)
        else:
            if date_val not in processed:
                to_process.append(f)

    logging.info(f"[{data_name}] Will process {len(to_process)} file(s)...")

    for json_file in to_process:
        date_val = parse_date_from_filename(json_file)
        try:
            # Use DuckDB to read & coalesce missing columns -> Arrow
            table = duckdb_read_json_with_defaults(json_file, arrow_schema)
            # Add the extra columns
            table = add_extra_columns(table, date_val)
            # Overwrite if re-run
            do_overwrite = (rerun_date == date_val)
            write_parquet_partitioned(table, stage_dir, overwrite=do_overwrite)
        except Exception as e:
            logging.error(f"Error processing {json_file}: {e}")

    storage.stop()


# ------------------------------------------------------------------------------
# 6. MAIN SCRIPT
# ------------------------------------------------------------------------------
def main():
    os.environ["NUMEXPR_MAX_THREADS"] = "16"

    logging.info("Loading schemas.yaml ...")
    current_dir = os.path.dirname(__file__)
    schema_file = os.path.join(current_dir, "..", "config", "stockinvest_us_schemas.yaml")
    schema_file = os.path.abspath(schema_file)

    schema_dict = load_schemas(schema_file)

    # If you need to re-run a specific date, specify here
    rerun_date_screener = None   # e.g. "20250108"
    rerun_date_trending = None   # e.g. "20250108"

    # Process screener_data -> uses 'screener' schema
    process_data_type(
        data_name="screener_data",
        schema_name="screener",
        schema_dict=schema_dict,
        rerun_date=rerun_date_screener
    )

    # Process trending_data -> uses 'trending' schema
    process_data_type(
        data_name="trending_data",
        schema_name="trending",
        schema_dict=schema_dict,
        rerun_date=rerun_date_trending
    )

    logging.info("All processing complete.")


if __name__ == "__main__":
    main()
