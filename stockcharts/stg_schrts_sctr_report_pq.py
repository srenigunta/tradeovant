import os
import json
import glob
import logging
import yaml
import pandas as pd
import tempfile
from tradeovant.imports.common_utils import LocalS3WithDirectory

def setup_logging(log_level):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config(config_path):
    """Load and parse YAML configuration file"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_processed_dates(delta_file):
    if os.path.exists(delta_file):
        with open(delta_file, "r") as f:
            processed = f.read().splitlines()
        return set(processed)
    else:
        return set()

def update_processed_dates(delta_file, date):
    with open(delta_file, "a") as f:
        f.write(date + "\n")

def build_local_path(storage, bucket, sub_path, filename):
    return os.path.join(storage.local_base_path, bucket, sub_path, filename)

def process_date(date, config, storage):
    logging.info(f"Processing date: {date}")

    sctr_conf = config["source"]["sctr_reports"]
    target_conf = config["target"]
    dfs = []
    extracted_date = None  # Common date field

    for classification, details in sctr_conf.items():
        file_path = build_local_path(storage, details["bucket"], details["sub_path"], f"{classification}_sctr_report_{date}.json")

        if os.path.exists(file_path):
            logging.info(f"Reading {classification} SCTR report file: {file_path}")
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                if not data or not isinstance(data, list):
                    logging.warning(f"Empty or invalid JSON data in {file_path}, skipping.")
                    continue

                # Extract the common date field from the first record
                if extracted_date is None:
                    extracted_date = data[0].get("date", None)

                # Process stock records
                stock_data = data[1:]  # Skip first entry (date)
                df = pd.DataFrame(stock_data)
                df["date"] = extracted_date  # Ensure date is set
                df["marketcaptype"] = details["marketcaptype"]  # Add classification type
                df["file_version_date"] = date  # Add partition column

                dfs.append(df)
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")

    # If no data was collected, skip this date
    if not dfs:
        logging.warning(f"No valid data found for {date}. Skipping processing.")
        return

    # Concatenate all available data
    df_final = pd.concat(dfs, ignore_index=True)

    # Ensure all target columns are present (fill missing ones)
    target_columns = target_conf["columns"]
    for col in target_columns:
        if col not in df_final.columns:
            df_final[col] = None  # Fill missing columns

    df_final = df_final[target_columns]  # Reorder columns

    # Define target directory structure
    partition_folder = f"file_version_date={date}"
    filename = f"schrts_sctr_reports_{date}.parquet"
    full_target_dir = os.path.join(target_conf["base_path"], partition_folder).replace("\\", "/")
    full_target_path = os.path.join(full_target_dir, filename).replace("\\", "/")

    # Create a temporary Parquet file
    try:
        temp_parquet_dir = tempfile.mkdtemp()  # Create temp dir
        temp_parquet_path = os.path.join(temp_parquet_dir, filename)

        logging.info(f"Writing consolidated data to Parquet file: {temp_parquet_path}")
        df_final.to_parquet(temp_parquet_path, index=False)  # REMOVE partition_cols

    except Exception as e:
        logging.error(f"Error writing temporary Parquet file for {date}: {e}")
        return

    # Upload to storage
    bucket = target_conf["bucket"]
    logging.info(f"Uploading Parquet file to bucket '{bucket}' with key '{full_target_path}'")
    try:
        storage.upload_file(temp_parquet_path, bucket, full_target_path)
    except Exception as e:
        logging.error(f"Error uploading Parquet file for {date}: {e}")
        return

    # Clean up temporary files and update processed dates
    os.remove(temp_parquet_path)
    logging.info(f"Finished processing date: {date}")
    update_processed_dates(config["processing"]["delta_tracking_file"], date)

def main():
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "stockcharts_sctr_reports_config.yaml")
    config = load_config(os.path.abspath(config_path))
    setup_logging(config["processing"].get("log_level", "INFO"))
    logging.info("Starting processing of SCTR reports.")

    storage = LocalS3WithDirectory()

    available_dates = set()
    for classification, details in config["source"]["sctr_reports"].items():
        bucket = details["bucket"]
        sub_path = details["sub_path"]
        full_path = os.path.join(storage.local_base_path, bucket, sub_path)
        pattern = os.path.join(full_path, details["pattern"])
        files = glob.glob(pattern)
        for filepath in files:
            base = os.path.basename(filepath)
            parts = base.split("_")
            if len(parts) >= 4:
                date_part = parts[-1].replace(".json", "")
                available_dates.add(date_part)

    logging.info(f"Found available dates: {available_dates}")

    delta_file = config["processing"]["delta_tracking_file"]
    processed_dates = get_processed_dates(delta_file)
    logging.info(f"Already processed dates: {processed_dates}")

    pending_dates = available_dates - processed_dates
    if not pending_dates:
        logging.info("No new files to process. Exiting gracefully.")
        return

    for date in sorted(pending_dates):
        process_date(date, config, storage)

    logging.info("Processing completed.")
    storage.stop()

if __name__ == "__main__":
    main()
