import yaml
import requests
import json
import logging
import random
from datetime import datetime
import os
import time
from tradeovant.imports.common_utils import LocalS3WithDirectory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("api_fetcher.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def load_config(config_path):
    """Load and parse YAML configuration file"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def construct_api_url(api_prefix, endpoint):
    """Construct full API URL from prefix and endpoint"""
    return f"{api_prefix}{endpoint}"


def process_category(category_name, category_config, headers, storage):
    """Process each API category and its endpoints"""
    current_date = datetime.now().strftime("%Y%m%d")

    try:
        # Extract storage paths
        raw_path = category_config['raw_sub_path']

        # Process each API endpoint
        for api in category_config['api_details']:
            api_name = api['api_name']
            api_endpoint = api['api_endpoint']
            api_child_endpoints = api['api_child_endpoint']

            if api_child_endpoints and api_child_endpoints.lower() != 'none':
                child_endpoints = api_child_endpoints.split(',')
            else:
                child_endpoints = [None]

            for child_endpoint in child_endpoints:
                try:
                    endpoint_to_use = child_endpoint.strip() if child_endpoint else api_endpoint
                    url = construct_api_url(api['api_prefix'], endpoint_to_use)
                    logger.info(f"Processing {api_name} - {endpoint_to_use} - {url}")

                    # Execute API request
                    response = requests.get(url, headers=headers, timeout=15)
                    response.raise_for_status()
                    data = response.json()

                    # Prepare file storage
                    if child_endpoint:
                        filename = f"{api_endpoint}_{child_endpoint}_{current_date}.json"
                    else:
                        filename = f"{api_name}_{current_date}.json"

                    file_key = f"{api_name}/json/{filename}"

                    data_file_path = os.path.join(storage.BASE_PATH, raw_path, file_key)

                    # Write to temporary file and upload
                    # Save "data" as JSON
                    with open(data_file_path, "w") as f:
                        json.dump(data, f, indent=4)
                    logging.info(f"Data successfully saved to {data_file_path}")
                    logger.info(f"Successfully stored: {file_key}")

                    # Introduce a random delay before the next request (5-10 seconds)
                    delay = random.randint(3, 7)
                    logger.info(f"Sleeping for {delay} seconds before the next request...")
                    time.sleep(delay)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Request failed for {api_name} - {endpoint_to_use}: {str(e)}")
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON response for {api_name} - {endpoint_to_use}")
                except Exception as e:
                    logger.error(f"Unexpected error processing {api_name} - {endpoint_to_use}: {str(e)}")

    except KeyError as e:
        logger.error(f"Missing required configuration in {category_name}: {str(e)}")


def main():
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "stockcharts_api_config.yaml")

    config = load_config(os.path.abspath(config_path))
    logger.info("Config file loaded...")

    headers = {
        "authority": "stockcharts.com",
        "method": "GET",
        "scheme": "https",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
        "x-requested-with": "XMLHttpRequest"
    }

    storage = LocalS3WithDirectory()
    logger.info("Storage initiated...")

    # Process all top-level categories except headers
    for category in config:
        if category == 'headers':
            continue
        logger.info(f"Processing category: {category}")
        process_category(category, config[category], headers, storage)


if __name__ == "__main__":
    main()