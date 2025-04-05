import boto3
import yaml
import os
import json
import logging
import time
from botocore.exceptions import ClientError


# Load configuration from YAML file
def load_config(config_path):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


# List files in the Polygon.io S3 bucket and return as a list
def list_s3_files(config):
    try:
        # Extract S3 configuration from YAML
        s3_config = config["s3_base"]
        access_key_id = s3_config["access_key_id"]
        secret_access_key = s3_config["secret_access_key"]
        endpoint_url = s3_config["s3_endpoint"]
        bucket_name = s3_config["bucket"]

        # Initialize S3 client with custom endpoint
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url
        )

        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        if "Contents" not in response:
            logging.info(f"No files found in bucket '{bucket_name}'")
            return []

        # Extract file details
        files = []
        for obj in response["Contents"]:
            file_info = {
                "Key": obj["Key"],
                "Size": obj["Size"],
                "LastModified": obj["LastModified"].isoformat(),
            }
            files.append(file_info)
            logging.info(
                f"Found file: {file_info['Key']} (Size: {file_info['Size']} bytes, Last Modified: {file_info['LastModified']})")

        # Handle pagination if more than 1000 objects
        while response.get("IsTruncated", False):
            continuation_token = response["NextContinuationToken"]
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                ContinuationToken=continuation_token
            )
            for obj in response["Contents"]:
                file_info = {
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"].isoformat(),
                }
                files.append(file_info)
                logging.info(
                    f"Found file: {file_info['Key']} (Size: {file_info['Size']} bytes, Last Modified: {file_info['LastModified']})")

        logging.info(f"Total files found: {len(files)}")
        return files

    except ClientError as e:
        logging.error(f"S3 error: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return []


# Save the list of files to a JSON file
def save_file_list_to_json(files, config):
    if not files:
        logging.info("No files to save to JSON")
        return

    # Use json_raw_path from local_storage in config
    output_dir = config["local_storage"]["json_raw_path"]
    os.makedirs(output_dir, exist_ok=True)

    # Construct filename with timestamp to avoid overwriting
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"s3_file_list_{timestamp}.json"
    output_path = os.path.join(output_dir, filename)

    with open(output_path, "w") as f:
        json.dump(files, f, indent=4)
    logging.info(f"Saved S3 file list to {output_path}")


# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load YAML configuration
    logging.info("Loading polygonio config...")
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "..", "config", "polygon_io_raw_config.yaml")
    config = load_config(os.path.abspath(config_path))

    # List files in the S3 bucket
    logging.info("Fetching available files from Polygon.io S3 bucket...")
    files = list_s3_files(config)

    # Save the list to a JSON file
    save_file_list_to_json(files, config)