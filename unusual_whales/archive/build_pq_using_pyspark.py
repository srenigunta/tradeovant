import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit
from tradeovant.imports.common_utils import LocalS3WithDirectory


def initialize_spark():
    """
    Initialize a SparkSession.
    """
    return SparkSession.builder \
        .appName("Stock Screener Processing") \
        .master("local[*]") \
        .config("spark.driver.memory", "150g") \
        .config("spark.sql.debug.maxToStringFields", "4000") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def list_csv_files(storage, bucket_name, bucket_key):
    """
    List all CSV files in the given storage location.
    """
    files = storage.list_files(bucket_name, bucket_key)
    return [os.path.basename(f) for f in files if f.endswith(".csv")]


def extract_date_from_filename(filename):
    """
    Extract date from the last 10 characters of the filename (before '.csv').
    Example filename: 'something_2025-01-01.csv'
    This function returns '2025-01-01' (10 chars).
    """
    return filename[-14:-4]  # Extract last 10 characters (YYYY-MM-DD)


def get_parquet_partitions(parquet_path):
    """
    Return a set of existing 'file_version_date' partitions.
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
    3) If the ZIP is in a subdirectory, we'll do a quick search in the source_path tree.
    """
    # The expected zip name replaces '.csv' with '.zip'
    expected_zip_name = csv_filename.replace(".csv", ".zip")

    # Create archive directory if not exists
    archive_dir = os.path.join(source_path, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    # Search for the zip file in source_path and its subdirectories
    zip_path = None
    for root, dirs, files in os.walk(source_path):
        # Skip the archive folder itself to avoid infinite loop
        if os.path.abspath(root) == os.path.abspath(archive_dir):
            continue
        if expected_zip_name in files:
            zip_path = os.path.join(root, expected_zip_name)
            break

    # If found, move it to the archive directory
    if zip_path and os.path.exists(zip_path):
        shutil.move(zip_path, os.path.join(archive_dir, expected_zip_name))
        print(f"Moved ZIP file to archive: {zip_path} -> {archive_dir}")
    else:
        print(f"ZIP file '{expected_zip_name}' not found under {source_path}. Skipping.")


def process_csv_files(storage,
                      bucket_name,
                      bucket_key,
                      parquet_path,
                      spark,
                      source_path,
                      rerun_dates=None):
    """
    Reads all CSV files from the given bucket/key, processes them into
    Parquet partitioned by file_version_date (with no hyphens),
    then moves the corresponding ZIP file to 'archive' and removes the CSV.
    """
    # 1) List CSV files
    csv_files = list_csv_files(storage, bucket_name, bucket_key)
    print(f"Found {len(csv_files)} CSV files in {bucket_name}/{bucket_key}.")

    # 2) Get existing parquet partitions
    processed_dates = get_parquet_partitions(parquet_path)
    print(f"Existing partitions: {processed_dates}")

    # 3) Determine which files to process
    to_process = []
    for file in csv_files:
        # Extract 'YYYY-MM-DD'
        file_date_with_hyphens = extract_date_from_filename(file)

        # Remove hyphens to get 'YYYYMMDD'
        file_date = file_date_with_hyphens.replace("-", "")

        if rerun_dates:
            if file_date in rerun_dates:
                to_process.append((file, file_date))
        else:
            # If we haven't processed this date yet, process now
            if file_date not in processed_dates:
                to_process.append((file, file_date))

    print(f"Files to process: {len(to_process)}")

    # 4) Process each file
    for file, file_date in to_process:
        print(f"Processing file: {file} with date: {file_date}")

        # Local CSV path (already extracted in the local S3-like directory)
        local_file_path = os.path.join(storage.BASE_PATH, bucket_name, bucket_key, file)

        # Read CSV into a Spark DataFrame
        df = spark.read.csv(local_file_path, header=True)

        # Add row id and file_version_date columns
        df = df.withColumn("rowid", monotonically_increasing_id()) \
               .withColumn("file_version_date", lit(file_date))

        # Write to Parquet with SNAPPY compression, partitioned by file_version_date
        df.coalesce(1).write.mode("append") \
                .option("compression", "snappy") \
                .partitionBy("file_version_date") \
                .parquet(parquet_path)

        print(f"File processed and written to parquet: {file}")

        # 5) Move the corresponding ZIP to 'archive'
        move_zip_file_to_archive(source_path, file)

        # 6) Remove the CSV file from the 'csv' directory
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Removed CSV file: {local_file_path}")


def rerun_files(storage, bucket_name, bucket_key, parquet_path, spark, source_path, rerun_dates):
    """
    Re-run processing for specific dates. Removes existing Parquet data for those dates
    and then reprocesses the CSV files for those dates.
    """
    print(f"Re-running for dates: {rerun_dates}")

    # Remove existing parquet data for each date
    for date in rerun_dates:
        date_dir = os.path.join(parquet_path, f"file_version_date={date}")
        if os.path.exists(date_dir):
            shutil.rmtree(date_dir)
            print(f"Removed existing parquet data for date: {date}")

    # Reprocess the files
    process_csv_files(storage, bucket_name, bucket_key, parquet_path, spark, source_path, rerun_dates)


if __name__ == "__main__":
    # Initialize the storage system
    storage = LocalS3WithDirectory()

    # Initialize Spark
    spark = initialize_spark()
    spark.conf.set("parquet.block.size", 128 * 1024 * 1024)  # 128 MB

    # List of source directories
    source_dirs = ["darkpool", "hotchains", "oichanges", "optionsflow", "stockscreener"]

    try:
        # Iterate through each source directory
        for source_dir in source_dirs:
            # Define the local source directory path (where ZIP files originally reside)
            source_path = rf"R:\daily_options_history\{source_dir}"

            # Define the bucket/key for CSV and Parquet
            bucket_name = "raw_store"
            bucket_key = f"whales\\{source_dir}\\csv"
            parquet_key = f"whales\\{source_dir}\\parquet"
            parquet_path = os.path.join(storage.BASE_PATH, bucket_name, parquet_key)

            # Process unprocessed data (default operation)
            process_csv_files(
                storage=storage,
                bucket_name=bucket_name,
                bucket_key=bucket_key,
                parquet_path=parquet_path,
                spark=spark,
                source_path=source_path
            )

            # Example: Re-run processing for specific dates if needed
            # rerun_dates = ["20250101"]  # e.g., "YYYYMMDD" format
            # rerun_files(storage, bucket_name, bucket_key, parquet_path, spark, source_path, rerun_dates)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Stop Spark session and storage service
        spark.stop()
        storage.stop()
