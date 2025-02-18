import os
import time
import logging
import pandas as pd
from finvizfinance.quote import finvizfinance
from finvizfinance.screener.overview import Overview
from tradeovant.imports.common_utils import LocalS3WithDirectory

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_version_date = time.strftime("%Y%m%d")

# Debug flag
debug = False  # Set to True for testing with a single ticker

def fetch_stocklist():
    """
    Fetch a list of stocks based on defined filters.
    """
    if not debug:
        filters = {'Option/Short': 'Optionable', 'Average Volume': 'Over 300K', 'Price': 'Over $2'}
    else:
        filters = {'Option/Short': 'Optionable', 'Relative Volume': 'Over 1', 'Market Cap.': 'Mega ($200bln and more)'}
    screener = Overview()
    screener.set_filter(filters_dict=filters)

    logging.info(f"Fetching filtered list: {filters}")
    try:
        stocks_list = screener.screener_view()
        time.sleep(30)
        if stocks_list.empty:
            raise ValueError("Stocks list is empty.")
    except Exception as e:
        logging.error("Failed to retrieve stocks list.")
        raise e

    return stocks_list


def scrape_stock_data(symbols, batch_size=20):
    """
    Scrape fundamentals and ratings for a list of stock symbols and save to local S3 as CSV.
    """
    fundamentals_data = []
    ratings_data = []

    logging.info("Fetching ticker fundamentals and ratings in batches.")
    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i + batch_size]
        for symbol in batch_symbols:
            try:
                # Fetch fundamentals
                stock = finvizfinance(symbol)
                fundament = stock.ticker_fundament()
                fundament['Ticker'] = symbol
                fundamentals_data.append(fundament)

                # Fetch ratings
                ratings = stock.ticker_outer_ratings()
                if isinstance(ratings, pd.DataFrame) and not ratings.empty:
                    ratings['Ticker'] = symbol
                    ratings_data.extend(ratings.to_dict('records'))
                elif isinstance(ratings, list):
                    for rating in ratings:
                        if isinstance(rating, dict):
                            rating['Ticker'] = symbol
                            ratings_data.append(rating)
                else:
                    logging.warning(f"Unexpected format for ratings data for {symbol}: {type(ratings)}")
                logging.info(f"Successfully fetched data for {symbol}.")
            except Exception as e:
                logging.warning(f"Failed to fetch data for {symbol}: {e}")

        # Respect server limits
        logging.info("Sleeping for 30 sec to avoid overloading the server.")
        time.sleep(30)

    # Convert to pandas DataFrames
    fundamentals_df = pd.DataFrame(fundamentals_data)
    ratings_df = pd.DataFrame(ratings_data)

    return fundamentals_df, ratings_df

if __name__ == "__main__":
    try:
        # Step 1: Scrape stock data and save to local S3 as CSV
        if not debug:
            stocks_df = pd.DataFrame(fetch_stocklist())
            symbols = stocks_df['Ticker'].tolist()
        else:
            symbols = ['AAPD']  # Debug mode with a single ticker
        fundamentals_df, ratings_df = scrape_stock_data(symbols)

        # Initialize storage
        storage = LocalS3WithDirectory()

        # Set local S3 bucket paths
        bucket_name = "raw_store"
        fundamentals_key = "finviz/fundamentals"
        ratings_key = "finviz/ratings"

        # Save to local S3 as CSV
        fundamentals_csv_path = os.path.join(storage.BASE_PATH, bucket_name, fundamentals_key, f"fundamentals_{file_version_date}.csv")
        ratings_csv_path = os.path.join(storage.BASE_PATH, bucket_name, ratings_key, f"ratings_{file_version_date}.csv")
        fundamentals_df.to_csv(fundamentals_csv_path, index=False)
        ratings_df.to_csv(ratings_csv_path, index=False)

        logging.info(f"Fundamentals and ratings saved to CSV")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        storage.stop()
