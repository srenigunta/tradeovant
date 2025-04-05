import os
import json
import logging
import requests
import random
from datetime import datetime
from tradeovant.imports.common_utils import LocalS3WithDirectory  # Correct import for LocalS3WithDirectory
import duckdb
import time

# Set up logging
def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    logging.getLogger("requests").setLevel(logging.WARNING)  # Reduce logging noise from requests


# Fetch tickers from the Parquet file using DuckDB
def fetch_tickers(parquet_file_path):
    query = """
    select distinct ticker
    from read_parquet('{parquet_file_path}', hive_partitioning = True)
    where file_version_date = (select max(file_version_date) 
                               from read_parquet('{parquet_file_path}', hive_partitioning = True))
    and is_index = 'f'
    and issue_type <> 'ETF';
    """
    con = duckdb.connect()
    df = con.execute(query.format(parquet_file_path=parquet_file_path)).fetchdf()
    return df['ticker'].tolist()


# Construct the URL for API data fetch
def construct_url(tickers):
    urlprefix = "https://www.tipranks.com/api/compare/stocks/?tickers="
    tickers_str = ",".join(tickers)
    return urlprefix + tickers_str


# Fetch JSON data from the API
def fetch_api_data(url):
    headers = {
        "host": "www.tipranks.com",
        "method": "GET",
        "scheme": "https",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        if response.status_code == 200:
            logging.info("Successfully processed batch...")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


# Check if the folder for the current date exists in the specified bucket using os module
def check_existing_data(storage, bucket, date):
    target_folder = f"tipranks/json/{date}/"
    folder_path = os.path.join(storage.local_base_path, bucket, target_folder)
    return os.path.exists(folder_path)  # True if folder exists, False otherwise


# Save both data and extraData to separate files
def save_data_to_raw_bucket(storage, bucket, data, extradata, date, filename_prefix="tipranks_screener"):
    # Ensure directory exists
    target_folder = f"tipranks/json/{date}/"
    folder_path = os.path.join(storage.local_base_path, bucket, target_folder)
    os.makedirs(folder_path, exist_ok=True)  # Create the directory if it doesn't exist

    # Create separate filenames for "data" and "extraData"
    data_filename = f"{filename_prefix}_data_{date}.json"
    extra_data_filename = f"{filename_prefix}_extradata_{date}.json"

    # File paths
    data_file_path = os.path.join(folder_path, data_filename)
    extra_data_file_path = os.path.join(folder_path, extra_data_filename)

    # Save "data" as JSON
    with open(data_file_path, "w") as f:
        json.dump(data, f, indent=4)
    logging.info(f"Data successfully saved to {data_file_path}")

    # Save "extraData" as JSON
    with open(extra_data_file_path, "w") as f:
        json.dump(extradata, f, indent=4)
    logging.info(f"ExtraData successfully saved to {extra_data_file_path}")


# Process batches of tickers and fetch data
def process_tickers_in_batches(tickers, batch_size, storage, bucket):
    current_date = datetime.today().strftime('%Y%m%d')

    # Step 1: Check if data already exists for the current date in the correct bucket
    if check_existing_data(storage, bucket, current_date):
        logging.info(f"Data for {current_date} already exists, skipping API data fetch.")
        return  # Skip processing if data already exists

    all_data = []  # Initialize an empty list to accumulate all data
    all_extra_data = []  # Initialize an empty list to accumulate extraData

    # Step 2: Process the tickers in batches (e.g., 100 tickers per batch)
    all_batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]

    for batch in all_batches:
        tickers_str = ",".join(batch)
        url = construct_url(batch)
        logging.info(f"Fetching data for tickers: {tickers_str}")

        # Fetch the data for the current batch of tickers
        data = fetch_api_data(url)
        if data:
            # Append the data to all_data
            all_data.extend(data.get("data", []))  # Add data from the current batch
            all_extra_data.extend(data.get("extraData", []))  # Add extra data if present
        delay = random.randint(10, 15)
        logging.info(f"Successfully fetched batch data, sleeping for {delay} seconds before the next request...")
        time.sleep(delay)

    # Step 3: After all batches are processed, save the accumulated data to two separate files
    save_data_to_raw_bucket(storage, bucket, all_data, all_extra_data, current_date,
                            filename_prefix="tipranks_screener")


def main():
    # Setup logging
    setup_logging()

    # Specify the raw bucket name where data should be stored
    raw_bucket = "raw_store"

    # Path to the Parquet file containing tickers
    parquet_file_path = "R:/local_bucket/raw_store/whales/optionscreener/parquet/*/*.parquet"

    # Instantiate the storage helper
    storage = LocalS3WithDirectory()

    # Step 1: Fetch tickers from the Parquet file
    tickers = fetch_tickers(parquet_file_path)
    logging.info(f"Fetched {len(tickers)} tickers from Parquet file.")

    # Step 2: Process the tickers in batches (e.g., 100 tickers per batch)
    process_tickers_in_batches(tickers, batch_size=50, storage=storage, bucket=raw_bucket)

    logging.info("All tickers processed successfully.")


if __name__ == "__main__":
    main()
