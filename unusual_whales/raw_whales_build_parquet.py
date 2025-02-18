import os
import shutil
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from tradeovant.imports.common_utils import LocalS3WithDirectory


def list_csv_files(storage, bucket_name, bucket_key):
    """
    List all CSV files in the given storage location.
    """
    files = storage.list_files(bucket_name, bucket_key)
    return [os.path.basename(f) for f in files if f.endswith(".csv")]


def extract_date_from_filename(filename):
    """
    Extract date from the last 10 characters of the filename (before '.csv').
    Example filename: 'something_2025-01-01.csv' -> '2025-01-01'
    """
    return filename[-14:-4]  # e.g. "2025-01-01"


def get_parquet_partitions(parquet_path):
    """
    Return a set of existing 'file_version_date' partitions (e.g., '20250101').
    Looks for subdirectories named 'file_version_date=<date>'.
    """
    if not os.path.exists(parquet_path):
        return set()

    partitions = set()
    for dir_name in os.listdir(parquet_path):
        if dir_name.startswith("file_version_date="):
            partitions.add(dir_name.split("=")[1])
    return partitions


def move_zip_file_to_archive(source_path, csv_filename):
    """
    1) Construct the expected ZIP filename from the CSV filename.
    2) Move that ZIP file from the source directory to an 'archive' subdirectory.
    3) If the ZIP is in a subdirectory, do a quick search under source_path.
    """
    expected_zip_name = csv_filename.replace(".csv", ".zip")
    archive_dir = os.path.join(source_path, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    zip_path = None
    for root, dirs, files in os.walk(source_path):
        if os.path.abspath(root) == os.path.abspath(archive_dir):
            # Skip searching inside 'archive' to avoid moving it again
            continue
        if expected_zip_name in files:
            zip_path = os.path.join(root, expected_zip_name)
            break

    if zip_path and os.path.exists(zip_path):
        shutil.move(zip_path, os.path.join(archive_dir, expected_zip_name))
        print(f"Moved ZIP file to archive: {zip_path} -> {archive_dir}")
    else:
        print(f"ZIP file '{expected_zip_name}' not found under {source_path}. Skipping.")


def read_csv_as_all_strings(csv_file):
    """
    Read the CSV using PyArrow, forcing ALL columns to string type
    (bypasses potential type-inference issues).
    """
    # 1. Read the header line to get column names.
    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
    column_names = first_line.split(",")

    # 2. Build a dict mapping each column to pa.string()
    column_types = {col: pa.string() for col in column_names}

    # 3. Read the CSV with forced string columns
    table = pacsv.read_csv(
        csv_file,
        read_options=pacsv.ReadOptions(use_threads=True),
        parse_options=pacsv.ParseOptions(delimiter=",", quote_char='"'),
        convert_options=pacsv.ConvertOptions(column_types=column_types)
    )

    return table


def add_columns(table, file_date):
    """
    Add two columns to the Arrow table:
     - rowid: a sequential ID (like Spark's monotonically_increasing_id)
     - file_version_date: the partition date (e.g. '20250101')
    """
    # Create rowid (0..N-1)
    row_count = table.num_rows
    rowid_array = pa.array(range(row_count), type=pa.int64())
    table = table.append_column("rowid", rowid_array)

    # Create file_version_date column
    fvd_array = pa.array([file_date] * row_count, type=pa.string())
    table = table.append_column("file_version_date", fvd_array)

    return table


def process_csv_files(storage,
                      bucket_name,
                      bucket_key,
                      parquet_path,
                      source_path,
                      source_dir,
                      rerun_dates=None):
    """
    Reads all CSV files from (bucket_name, bucket_key), converts them to
    Parquet partitioned by file_version_date (YYYYMMDD), moves the corresponding ZIP,
    and removes the CSV. If rerun_dates is None, only processes new dates;
    if rerun_dates is a list, reprocesses those date(s).
    """
    # 1) List CSV files
    csv_files = list_csv_files(storage, bucket_name, bucket_key)
    print(f"Found {len(csv_files)} CSV files in {bucket_name}/{bucket_key}.")

    # 2) Get existing Parquet partitions
    processed_dates = get_parquet_partitions(parquet_path)
    print(f"Existing partitions: {len(processed_dates)}")

    # 3) Determine files to process
    to_process = []
    for file in csv_files:
        # e.g., '2025-01-01'
        file_date_with_hyphens = extract_date_from_filename(file)
        # e.g., '20250101'
        file_date = file_date_with_hyphens.replace("-", "")

        if rerun_dates:
            # Process only if it's in the rerun list
            if file_date in rerun_dates:
                to_process.append((file, file_date))
        else:
            # Process if we haven't done this date yet
            if file_date not in processed_dates:
                to_process.append((file, file_date))

    print(f"Files to process: {len(to_process)}")

    # 4) For each file, read CSV -> Arrow -> Partitioned Parquet
    for file, file_date in to_process:
        print(f"Processing file: {file} with date: {file_date}")

        local_file_path = os.path.join(storage.BASE_PATH, bucket_name, bucket_key, file)

        # Read CSV with forced string columns
        arrow_table = read_csv_as_all_strings(local_file_path)
        # Add rowid & file_version_date
        arrow_table = add_columns(arrow_table, file_date)

        # Create partition subdirectory (e.g. /.../file_version_date=20250101)
        partition_dir = os.path.join(parquet_path, f"file_version_date={file_date}")
        os.makedirs(partition_dir, exist_ok=True)

        # Create a Parquet filename for this CSV
        # e.g. "bot-eod-report-2024-08-23.parquet" or something unique
        base_name = source_dir
        out_file = os.path.join(partition_dir, f"{base_name}.parquet")

        # Write the Parquet file (overwrite if it already exists)
        pq.write_table(arrow_table, out_file, compression="snappy")
        print(f"Wrote Parquet: {out_file}")

        # 5) Move the ZIP to archive
        move_zip_file_to_archive(source_path, file)

        # 6) Remove the CSV file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Removed CSV file: {local_file_path}")


def rerun_files(storage, bucket_name, bucket_key, parquet_path, source_path, source_dir, rerun_dates ):
    """
    Remove existing Parquet partitions for specified dates and reprocess them.
    """
    print(f"Re-running for dates: {rerun_dates}")
    for date in rerun_dates:
        date_dir = os.path.join(parquet_path, f"file_version_date={date}")
        if os.path.exists(date_dir):
            shutil.rmtree(date_dir)
            print(f"Removed existing parquet data for date: {date}")

    # After removing old data, we reprocess the CSVs for these dates
    process_csv_files(storage, bucket_name, bucket_key, parquet_path, source_path, source_dir, rerun_dates)


if __name__ == "__main__":

    # 1) Initialize local S3-like storage
    storage = LocalS3WithDirectory()

    # 2) List of source directories
    source_dirs = ["darkpool", "hotchains", "oichanges", "optionsflow", "stockscreener"]

    try:
        # 3) Iterate through each source directory
        for source_dir in source_dirs:
            source_path = rf"R:\daily_options_history\{source_dir}"

            bucket_name = "raw_store"
            bucket_key = f"whales\\{source_dir}\\csv"
            parquet_key = f"whales\\{source_dir}\\parquet"
            parquet_path = os.path.join(storage.BASE_PATH, bucket_name, parquet_key)

            # Process unprocessed data
            process_csv_files(
                storage=storage,
                bucket_name=bucket_name,
                bucket_key=bucket_key,
                parquet_path=parquet_path,
                source_path=source_path,
                source_dir=source_dir
            )

            # Example: re-run a specific date
            # rerun_dates = ["20250101"]
            # rerun_files(storage, bucket_name, bucket_key, parquet_path, source_path, rerun_dates, source_dir)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 4) Stop the storage service if needed
        storage.stop()
