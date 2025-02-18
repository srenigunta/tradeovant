import logging
import sys
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential
from curl_cffi import requests
import pyarrow.parquet as pq
from tradeovant.imports.common_utils import LocalS3WithDirectory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class ChartDownloader:
    def __init__(self, config):
        self.config = config
        self.storage = LocalS3WithDirectory()
        self.base_path = config['base_path']
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Priority": "u=0, i",
            "Sec-Ch-Ua": '"Not A(Brand";v="8", "Chromium";v="132", "Microsoft Edge";v="132"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://stockcharts.com/"
        }
        self.session = None

    def _init_session(self):
        """Create a new session per thread"""
        session = requests.Session()
        session.headers.update(self.headers)
        return session

    def _get_latest_partition(self):
        """Find the latest partition directory based on file_version_date"""
        bronze_path = os.path.join(
            self.base_path,
            self.config['bronze_bucket'],
            self.config['bronze_key_base']
        )

        partitions = []
        for entry in os.listdir(bronze_path):
            if entry.startswith("file_version_date="):
                date_str = entry.split('=')[1]
                try:
                    partition_date = datetime.strptime(date_str, "%Y%m%d")
                    partitions.append((partition_date, entry))
                except ValueError:
                    continue

        if not partitions:
            raise ValueError("No valid partitions found")

        latest_partition = max(partitions, key=lambda x: x[0])
        return os.path.join(bronze_path, latest_partition[1])

    def _get_symbols_from_parquet(self):
        """Load distinct symbols from latest parquet partition"""
        partition_dir = self._get_latest_partition()
        parquet_path = os.path.join(partition_dir, self.config['parquet_file'])

        table = pq.read_table(parquet_path)
        symbols = table.column('symbol').unique().to_pylist()
        logging.info(f"Loaded {len(symbols)} unique symbols from {parquet_path}")
        return symbols

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _download_with_retry(self, symbol):
        """Retryable download function for a single symbol"""
        try:
            session = self._init_session()
            url = self.config['url_template'].format(symbol=symbol)
            target_dir = os.path.join(
                self.base_path,
                self.config['target_bucket'],
                self.config['target_key_base']
            )
            os.makedirs(target_dir, exist_ok=True)
            filename = os.path.join(target_dir, f"{symbol}.png")

            response = session.get(
                url,
                impersonate="chrome110",
                timeout=self.config['request_timeout']
            )

            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                return (symbol, True)
            else:
                logging.error(f"HTTP Error {response.status_code} for {symbol}")
                return (symbol, False)

        except Exception as e:
            logging.error(f"Critical error for {symbol}: {str(e)}")
            raise

    def _process_batch(self, symbols):
        """Process a batch of symbols with ThreadPoolExecutor"""
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = {executor.submit(self._download_with_retry, symbol): symbol for symbol in symbols}
            results = []

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.error(f"Unhandled exception for {symbol}: {str(e)}")
                    results.append((symbol, False))

        return results

    def run(self):
        """Main execution flow with error handling"""
        try:
            symbols = []

            if self.config['debug']:
                symbols = [self.config['test_symbol']]
                logging.info(f"Debug mode enabled. Testing with symbol: {self.config['test_symbol']}")
            else:
                try:
                    symbols = self._get_symbols_from_parquet()
                except Exception as e:
                    logging.error(f"Error loading symbols: {str(e)}")
                    sys.exit(1)

            total_symbols = len(symbols)
            batch_size = self.config['batch_size']
            success_count = 0

            for batch_num, i in enumerate(range(0, total_symbols, batch_size), 1):
                batch_symbols = symbols[i:i + batch_size]
                logging.info(
                    f"Processing batch {batch_num}/{(total_symbols // batch_size) + 1} ({len(batch_symbols)} symbols)")

                try:
                    batch_results = self._process_batch(batch_symbols)
                    batch_success = sum(1 for _, status in batch_results if status)
                    success_count += batch_success

                    logging.info(f"Batch {batch_num} complete: {batch_success}/{len(batch_symbols)} succeeded")

                    if i + batch_size < total_symbols:
                        logging.info(f"Waiting {self.config['batch_delay']}s before next batch...")
                        time.sleep(self.config['batch_delay'])

                except KeyboardInterrupt:
                    logging.info("Process interrupted by user")
                    sys.exit(1)
                except Exception as e:
                    logging.error(f"Critical batch error: {str(e)}")
                    sys.exit(1)

            logging.info(f"Process completed. Total downloaded: {success_count}/{total_symbols}")

        finally:
            self.storage.stop()


if __name__ == "__main__":
    CONFIG = {
        "debug": False,
        "base_path": r"R:\local_bucket",
        "bronze_bucket": "bronze_store",
        "bronze_key_base": "finviz/fundamentals",
        "parquet_file": "brz_fundamentals.parquet",
        "target_bucket": "raw_store",
        "target_key_base": "stockcharts/pof_charts",
        "url_template": "https://stockcharts.com/pnf/chart?c={symbol},PATLDDYRBR[PA][D][F1!3!!!2!20]&r=3895&pnf=y",
        "test_symbol": "DIS",
        "request_timeout": 20,
        "max_workers": 10,      # Number of parallel downloads
        "batch_size": 200,      # Symbols per batch
        "batch_delay": 5,       # Seconds between batches
    }

    downloader = ChartDownloader(CONFIG)
    downloader.run()