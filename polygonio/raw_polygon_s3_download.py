import boto3
import yaml
import os
import logging
from botocore.exceptions import ClientError

# Load configuration from YAML file
def load_config(config_path):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

# Download a single file from S3
def download_s3_file(config, file_key, output_dir):
    try:
        # Extract S3 configuration from YAML
        s3_config = config["s3_base"]
        access_key_id = s3_config["access_key_id"]
        secret_access_key = s3_config["secret_access_key"]
        endpoint_url = s3_config["s3_endpoint"]
        bucket_name = s3_config["bucket"]

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url
        )

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, os.path.basename(file_key))

        # Download the file
        s3_client.download_file(bucket_name, file_key, output_path)
        logging.info(f"Downloaded {file_key} to {output_path}")
        return True
    except ClientError as e:
        logging.error(f"Error downloading {file_key}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False

# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load YAML configuration
    logging.info("Loading polygonio config...")
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "polygon_io_raw_config.yaml")
    config = load_config(os.path.abspath(config_path))

    # Specify the sample file and output directory
    sample_file_key = "us_stocks_sip/day_aggs_v1/2025/03/2025-03-04.csv.gz"
    output_dir = config["local_storage"]["json_raw_path"]  # Reuse json_raw_path for simplicity

    # Download the sample file
    logging.info(f"Attempting to download sample file: {sample_file_key}")
    success = download_s3_file(config, sample_file_key, output_dir)
    if not success:
        logging.info("Download failed. Check credentials or file availability.")