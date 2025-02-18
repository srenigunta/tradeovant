import logging
import os
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from tradeovant.imports.common_utils import LocalS3WithDirectory  # Assuming same utility as reference

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_raw_partitions(raw_base_path):
    """Get all date partitions from raw bucket (YYYYMMDD format)"""
    date_dirs = []
    for entry in os.listdir(raw_base_path):
        if os.path.isdir(os.path.join(raw_base_path, entry)) and len(entry) == 8:
            date_dirs.append(entry)
    return sorted(date_dirs)


def get_processed_dates(stage_dir):
    """Get already processed dates from stage directory"""
    if not os.path.exists(stage_dir):
        return set()

    processed = set()
    for name in os.listdir(stage_dir):
        if name.startswith("file_version_date="):
            processed.add(name.split("=")[1])
    return processed


def process_date_partition(raw_dir, date_val, stage_dir):
    """Process a single date partition using DuckDB SQL"""
    # Build file paths
    extradata_path = os.path.join(raw_dir, date_val, f"tipranks_screener_extradata_{date_val}.json")
    screener_path = os.path.join(raw_dir, date_val, f"tipranks_screener_data_{date_val}.json")

    if not (os.path.exists(extradata_path) and os.path.exists(screener_path)):
        logging.warning(f"Missing files for date {date_val}")
        return

    # DuckDB SQL Query
    query = f"""
    SELECT DISTINCT
        sed.ticker,
        sd.analystConsensus.consensus as analyst_consensus,
        sd.analystConsensus.distribution.buy as buy_count,
        sd.analystConsensus.distribution.hold as hold_count,
        sd.analystConsensus.distribution.sell as sell_count,
        COALESCE(research.tipRanksScore, 0) as tipRanksScore,
        research.priceTarget as tr_avg_price_target,
        COALESCE(research.analystConsensus, 0) as analyst_consensus_rating,
        COALESCE(research.bloggerConsensus, 0) as blogger_consensus_rating,
        COALESCE(research.insiderSignal, 0) as tr_insider_rating,
        COALESCE(research.hedgeFundSignal, 0) as tr_hedge_fund_rating,
        COALESCE(research.newsSentiment, 0) as tr_news_rating,
        COALESCE(research.investorSentiment, 0) as tr_investor_rating,
        COALESCE(research.rawInsiderScore, 0) as tr_insider_score,
        COALESCE(research.rawHedgeFundsScore, 0) as tr_hedge_fund_score,
        COALESCE(research.rawNewsSentiment, 0) as tr_news_score,
        COALESCE(research.rawInvestorScore, 0) as tr_investor_score,
        research.highestPriceTarget as tr_high_pricetarget,
        research.lowestPriceTarget as tr_low_pricetarget
    FROM read_json('{extradata_path}', format='array') sed
    INNER JOIN read_json('{screener_path}', format='array') sd
        ON sd.ticker = sed.ticker
    """

    # Execute query and get Arrow table
    conn = duckdb.connect()
    try:
        arrow_table = conn.execute(query).arrow()

        # Add file_version_date column
        date_array = pa.array([date_val] * arrow_table.num_rows, pa.string())
        arrow_table = arrow_table.append_column("file_version_date", date_array)

        # Write partitioned Parquet
        pq.write_to_dataset(
            arrow_table,
            root_path=stage_dir,
            partition_cols=["file_version_date"],
            compression="snappy",
            existing_data_behavior="delete_matching"
        )
        logging.info(f"Processed date {date_val} successfully")

    except Exception as e:
        logging.error(f"Failed processing {date_val}: {str(e)}")
    finally:
        conn.close()


def main():
    storage = LocalS3WithDirectory()

    # Configure paths
    raw_base_path = os.path.join(storage.BASE_PATH, "raw_store", "tipranks", "json")
    stage_dir = os.path.join(storage.BASE_PATH, "stage_store", "tipranks", "screener")

    # Get date partitions
    raw_dates = get_raw_partitions(raw_base_path)
    processed_dates = get_processed_dates(stage_dir)

    logging.info(f"Found {len(raw_dates)} raw partitions, {len(processed_dates)} already processed")

    # Process new dates
    for date_val in raw_dates:
        if date_val not in processed_dates:
            logging.info(f"Processing date: {date_val}")
            process_date_partition(raw_base_path, date_val, stage_dir)
        else:
            logging.debug(f"Skipping already processed date: {date_val}")

    storage.stop()
    logging.info("Processing complete")


if __name__ == "__main__":
    main()