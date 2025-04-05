import requests
import yaml
import time
import os
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import pytz


# Load configuration from YAML file
def load_config(config_path):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


# Generate date windows for iteration (max 70 days per window)
def generate_date_windows(start_date, end_date, max_days=70):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1  # Include end date

    if total_days <= max_days:
        return [(start_date, end_date)]

    num_windows = max(1, (total_days + max_days - 1) // max_days)  # Ceiling division
    days_per_window = total_days // num_windows  # Balance days
    remainder = total_days % num_windows  # Extra days to distribute

    windows = []
    current_start = start
    for i in range(num_windows):
        extra_day = 1 if i < remainder else 0  # Distribute remainder days
        window_days = days_per_window + extra_day
        current_end = current_start + timedelta(days=window_days - 1)

        # Ensure we don’t exceed the end date
        if current_end > end:
            current_end = end

        windows.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))
        current_start = current_end + timedelta(days=1)

        if current_start > end:
            break

    return windows


# Fetch 1-minute intraday data from Polygon.io
def fetch_intraday_data(ticker, start_date, end_date, multiplier, timespan, config):
    base_url = config["api_base"]["base_url"]
    api_key = config["api_base"]["api_key"]
    endpoint = config["api_base"]["endpoints"]["market"]["aggregates"]

    url = f"{base_url}{endpoint}{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        logging.info(
            f"Error fetching {ticker} from {start_date} to {end_date}: {response.status_code} - {response.text}")
        return None


# Save raw JSON data to local storage
def save_raw_json(data, ticker, start_date, end_date, multiplier, timespan, config):
    if data:
        output_dir = config["local_storage"]["json_raw_path"]
        os.makedirs(output_dir, exist_ok=True)

        filename = f"{ticker}_{start_date}_{end_date}_{multiplier}_{timespan}.json"
        output_path = os.path.join(output_dir, filename)

        with open(output_path, "w") as f:
            json.dump(data, f)
        logging.info(f"Saved raw JSON for {ticker} to {output_path}")
    else:
        logging.info(f"No JSON data to save for {ticker}")


# Process data with PyArrow and save to Parquet with ET conversion and trading hours flag
def process_and_save_to_parquet(data, ticker, start_date, end_date, multiplier, timespan, config):
    if data and "results" in data:
        utc_tz = pytz.UTC
        et_tz = pytz.timezone("America/New_York")

        results = data["results"]
        dates_utc = [datetime.fromtimestamp(r["t"] / 1000, tz=utc_tz) for r in results]
        dates_et = [dt.astimezone(et_tz) for dt in dates_utc]
        dates_et_str = [dt.strftime("%Y-%m-%d %H:%M:%S%z") for dt in dates_et]

        trading_start_hour, trading_start_minute = 9, 30
        trading_end_hour, trading_end_minute = 16, 0

        trading_flags = []
        for dt in dates_et:
            hour, minute = dt.hour, dt.minute
            if (hour > trading_start_hour or (hour == trading_start_hour and minute >= trading_start_minute)) and \
                    (hour < trading_end_hour or (hour == trading_end_hour and minute <= trading_end_minute)):
                trading_flags.append("Y")
            else:
                trading_flags.append("N")

        schema = pa.schema([
            ("date", pa.string()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("regular_trading", pa.string())
        ])

        table = pa.Table.from_pydict({
            "date": dates_et_str,
            "open": [r["o"] for r in results],
            "high": [r["h"] for r in results],
            "low": [r["l"] for r in results],
            "close": [r["c"] for r in results],
            "volume": [r["v"] for r in results],
            "regular_trading": trading_flags
        }, schema=schema)

        output_dir = config["local_storage"]["parquet_raw_path"]
        os.makedirs(output_dir, exist_ok=True)

        filename = f"{ticker}_{start_date}_{end_date}_{multiplier}_{timespan}.parquet"
        output_path = os.path.join(output_dir, filename)

        pq.write_table(table, output_path)
        logging.info(f"Saved {ticker} intraday data to Parquet at {output_path}")
    else:
        logging.info(f"No data to process for {ticker}")


# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load YAML configuration
    logging.info("Loading polygonio config...")
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "polygon_io_raw_config.yaml")
    config = load_config(os.path.abspath(config_path))

    # Example: Fetch 1-minute intraday data for multiple stocks over 2 years
    tickers = ["SPY"]
    start_date = "2023-03-06"  # 2 years back from 2025-03-01
    end_date = "2025-03-05"
    multiplier = "1"
    timespan = "minute"

    for ticker in tickers:
        # Generate date windows (max 70 days each)
        date_windows = generate_date_windows(start_date, end_date, max_days=70)
        logging.info(f"Fetching {ticker} data in {len(date_windows)} windows")

        for window_start, window_end in date_windows:
            logging.info(f"Processing {ticker} from {window_start} to {window_end}")
            data = fetch_intraday_data(ticker, window_start, window_end, multiplier, timespan, config)
            if data:
                save_raw_json(data, ticker, window_start, window_end, multiplier, timespan, config)
                process_and_save_to_parquet(data, ticker, window_start, window_end, multiplier, timespan, config)
            time.sleep(12)  # Respect free tier rate limit (5 requests/minute)