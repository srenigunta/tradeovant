import os
import json
import glob
import logging
import shutil
import duckdb
import pandas as pd
import tempfile
import yaml

# Import your storage module
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
    """
    Build a local file path based on the storage's base path.
    """
    return os.path.join(storage.BASE_PATH, bucket, sub_path, filename)


def process_date(date, config, storage):
    logging.info(f"Processing date: {date}")

    # Build file paths for the two source types using bucket and sub_path
    future_conf = config["source"]["future_earnings"]
    reported_conf = config["source"]["reported_earnings"]

    future_filename = f"future_earnings_{date}.json"
    reported_filename = f"reported_earnings_{date}.json"

    future_file = build_local_path(storage, future_conf["bucket"], future_conf["sub_path"], future_filename)
    reported_file = build_local_path(storage, reported_conf["bucket"], reported_conf["sub_path"], reported_filename)

    df_future = pd.DataFrame()
    df_reported = pd.DataFrame()

    # Process future earnings file
    if os.path.exists(future_file):
        logging.info(f"Reading future earnings file: {future_file}")
        try:
            with open(future_file, "r") as f:
                data = json.load(f)
            df_future = pd.DataFrame(data.get("earningsDates", []))
            # Ensure that Symbol is present
            if "Symbol" not in df_future.columns:
                logging.error("Future earnings file is missing the Symbol column.")
                return
            # Only select and rename columns as per columns_map
            cols_map = future_conf["columns_map"]
            available_cols = {k: v for k, v in cols_map.items() if k in df_future.columns}
            df_future = df_future[list(available_cols.keys())]
            df_future.rename(columns=available_cols, inplace=True)
        except Exception as e:
            logging.error(f"Error reading future file {future_file}: {e}")
    else:
        logging.warning(f"Future earnings file for date {date} not found.")

    # Process reported earnings file
    if os.path.exists(reported_file):
        logging.info(f"Reading reported earnings file: {reported_file}")
        try:
            with open(reported_file, "r") as f:
                data = json.load(f)
            df_reported = pd.DataFrame(data.get("earningsDates", []))
            # Ensure Symbol is present
            if "Symbol" not in df_reported.columns:
                logging.error("Reported earnings file is missing the Symbol column.")
                return
            required_cols = reported_conf["columns"]
            df_reported = df_reported[[col for col in required_cols if col in df_reported.columns]]
        except Exception as e:
            logging.error(f"Error reading reported file {reported_file}: {e}")
    else:
        logging.warning(f"Reported earnings file for date {date} not found.")

    if df_future.empty and df_reported.empty:
        logging.warning(f"No data available for date {date}. Skipping processing.")
        return

    # Create empty DataFrames if one source is missing
    target_cols = config["target"]["columns"]
    if df_reported.empty:
        rep_cols = reported_conf["columns"]
        df_reported = pd.DataFrame(columns=rep_cols)
    if df_future.empty:
        fut_cols = list(future_conf["columns_map"].values())
        df_future = pd.DataFrame(columns=fut_cols)

    # Join the two datasets on the Symbol
    con = duckdb.connect(database=":memory:")
    con.register("reported", df_reported)
    con.register("future", df_future)

    select_expr = """
      COALESCE(r.Symbol, f.Symbol) AS Symbol,
      r.ActualNetIncome,
      r.ActualEPS,
      r.EstimatedNetIncome,
      r.FiscalPeriod,
      r.EstimatedEPS,
      r.EarningsDate,
      r.EstimatedSales,
      r.ActualSales,
      f.Upcoming_EarningsDate,
      f.Upcoming_EarningsType,
      f.Upcoming_EstimatedSales,
      f.Upcoming_EstimatedNetIncome,
      f.Upcoming_EstimatedEPS
    """
    join_query = f"""
      SELECT {select_expr}
      FROM reported r
      FULL OUTER JOIN future f
      ON r.Symbol = f.Symbol
    """
    try:
        logging.info("Executing join query.")
        df_combined = con.execute(join_query).fetchdf()
    except Exception as e:
        logging.error(f"Error during join query for date {date}: {e}")
        return

    # Reorder columns to match target schema
    df_combined = df_combined[[col for col in target_cols if col in df_combined.columns]]

    # Build target S3 key (stage_store) with partition folder structure:
    # stockcharts/earnings/file_version_date=YYYYMMDD/schrts_earnings_YYYYMMDD.parquet

    target_conf = config["target"]
    bucket = target_conf["bucket"]
    file_path = target_conf["base_path"]
    partition_folder = f"file_version_date={date}"
    filename = target_conf["pattern"].format(date=date)
    target_path = os.path.join(storage.BASE_PATH, bucket, file_path, partition_folder)

    if os.path.exists(target_path):
        shutil.rmtree(target_path)
        os.makedirs(target_path, exist_ok=True)
        logging.info(f"Removed existing partition dir: {target_path}")
    else:
        os.makedirs(target_path, exist_ok=True)
    out_file = os.path.join(target_path, f"{filename}")
    df_combined.to_parquet(out_file, index=False)

    logging.info(f"Final parquet file to bucket '{bucket}' with key '{filename}'")
    # Mark the date as processed
    delta_file = config["processing"]["delta_tracking_file"]
    update_processed_dates(delta_file, date)


def main():
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "stockcharts_earnings_config.yaml")
    config = load_config(os.path.abspath(config_path))
    setup_logging(config["processing"].get("log_level", "INFO"))
    logging.info("Starting processing of earnings data.")

    # Initialize storage using your custom module
    storage = LocalS3WithDirectory()

    # List source files from both future and reported earnings using the storage paths.
    available_dates = set()
    for key in ["future_earnings", "reported_earnings"]:
        source_conf = config["source"][key]
        bucket = source_conf["bucket"]
        sub_path = source_conf["sub_path"]
        full_path = os.path.join(storage.local_base_path, bucket, sub_path)
        pattern = os.path.join(full_path, source_conf["pattern"])
        files = glob.glob(pattern)
        for filepath in files:
            base = os.path.basename(filepath)
            parts = base.split("_")
            if len(parts) >= 3:
                date_part = parts[-1].replace(".json", "")
                available_dates.add(date_part)

    logging.info(f"Found available dates: {len(available_dates)}")

    delta_file = config["processing"]["delta_tracking_file"]
    processed_dates = get_processed_dates(delta_file)
    logging.info(f"Already processed dates: {len(processed_dates)}")

    pending_dates = available_dates - processed_dates
    if not pending_dates:
        logging.info("No new files to process. Exiting...")
        return

    for date in sorted(pending_dates):
        process_date(date, config, storage)

    logging.info("Processing completed.")
    storage.stop()  # Stop the mock S3 service when done


if __name__ == "__main__":
    main()
