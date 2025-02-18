import os
import shutil
import json
import zipfile
from pathlib import Path
import boto3
from moto import mock_s3


class LocalS3WithDirectory:
    BASE_PATH = r"R:\local_bucket"  # Define base path within the class
    BUCKETS = ["raw_store", "stage_store", "temp_store", "bronze_store", "silver_store", "gold_store"]  # Define buckets
    def __init__(self, region_name="us-east-1"):
        """
        Initialize the local storage and mock S3 service.

        Parameters:
            region_name (str): AWS region name for the mock S3 service.
        """
        self.local_base_path = Path(self.BASE_PATH)
        self.region_name = region_name
        self.mock = mock_s3()
        self.mock.start()
        self.s3 = boto3.client("s3", region_name=self.region_name)

        # Automatically start and initialize buckets
        self._initialize_buckets()

    def stop(self):
        """
        Stop the moto mock context explicitly.
        """
        self.mock.stop()

    def _initialize_buckets(self):
        """
        Initialize the buckets and corresponding directories.
        Ensures that buckets exist in both the mock S3 and local directory.
        """
        os.makedirs(self.local_base_path, exist_ok=True)  # Ensure the base path exists

        # Get the list of existing buckets in the mock S3
        existing_buckets = {bucket["Name"] for bucket in self.s3.list_buckets().get("Buckets", [])}

        for bucket_name in self.BUCKETS:
            if bucket_name not in existing_buckets:
                # Create the bucket in mock S3 only if it doesn't already exist
                self.s3.create_bucket(Bucket=bucket_name)

            # Ensure the local directory exists for the bucket
            bucket_path = self.local_base_path / bucket_name
            if not bucket_path.exists():
                bucket_path.mkdir(parents=True, exist_ok=True)

    from pathlib import Path
    import shutil
    import os

    def upload_file(self, local_file, bucket_name, bucket_key):
        """
        Upload a file to the mock S3 bucket and local directory.

        Parameters:
            local_file (str): Path to the local file to upload.
            bucket_name (str): Name of the bucket.
            bucket_key (str): Key (path) to store the file in the bucket.
        """
        # Manually build the paths
        target_path = os.path.join(self.local_base_path, bucket_name, bucket_key)

        # Ensure the target directory exists
        target_dir = os.path.dirname(target_path)  # Get the directory part
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            print(f"Created target directory: {target_dir}")

        # Check if the local file exists before trying to copy
        if not os.path.exists(local_file):
            print(f"Local file does not exist: {local_file}")
            return

        # Copy the file to the target directory
        try:
            shutil.copy(local_file, target_path)
            print(f"File copied to local storage: {target_path}")
        except Exception as e:
            print(f"Error copying file: {e}")
            return

        # Upload the file to S3
        try:
            self.s3.upload_file(local_file, bucket_name, bucket_key)
            print(f"Uploaded to mock S3: {bucket_name}/{bucket_key}")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            return

    def list_files(self, bucket_name, bucket_key=""):
        """
        List files in the mock S3 bucket and local directory.

        Parameters:
            bucket_name (str): Name of the bucket.
            bucket_key (str): Key (path) to search within the bucket.
        """
        bucket_path = self.local_base_path / bucket_name / bucket_key
        files = [str(p.relative_to(self.local_base_path / bucket_name)) for p in bucket_path.rglob("*") if p.is_file()]
        print(f"Files in bucket '{bucket_name}/{bucket_key}': {files}")
        return files

    def download_file(self, bucket_name, bucket_key, local_path):
        """
        Download a file from the mock S3 bucket and local directory.

        Parameters:
            bucket_name (str): Name of the bucket.
            bucket_key (str): Key (path) of the file to download.
            local_path (str): Path to save the downloaded file.
        """
        source_path = self.local_base_path / bucket_name / bucket_key
        shutil.copy(source_path, local_path)
        print(f"Downloaded from local storage: {source_path} to {local_path}")

        self.s3.download_file(bucket_name, bucket_key, local_path)
        print(f"Downloaded from mock S3: {bucket_name}/{bucket_key} to {local_path}")

    def move_and_unzip(self, source_path, bucket_name, bucket_key):
        """
        Move and unzip files from source directory to bucket directory.

        Parameters:
            source_path (str): Path to the source directory containing zip files.
            bucket_name (str): Name of the target bucket.
            bucket_key (str): Key (path) within the bucket to move files to.
        """
        source_path = Path(source_path)
        target_path = self.local_base_path / bucket_name / bucket_key
        unzipped_path = target_path / "unzipped"

        # Ensure target paths exist
        target_path.mkdir(parents=True, exist_ok=True)
        unzipped_path.mkdir(parents=True, exist_ok=True)

        # Process each zip file in the source directory
        for zip_file in source_path.glob("*.zip"):
            try:
                # Move the zip file
                shutil.move(str(zip_file), target_path / zip_file.name)
                print(f"Moved: {zip_file} to {target_path / zip_file.name}")

                # Unzip the file
                with zipfile.ZipFile(target_path / zip_file.name, 'r') as zf:
                    zf.extractall(unzipped_path)
                print(f"Unzipped: {zip_file.name} to {unzipped_path}")
            except Exception as e:
                print(f"Error processing {zip_file}: {e}")

    def fetch_and_extract(self, source_path, bucket_name, bucket_key):
        """
        Fetch and extract files dynamically from source zip files to the target directory if not present.

        Parameters:
            source_path (str): Path to the source directory containing zip files.
            bucket_name (str): Name of the target bucket.
            bucket_key (str): Key (path) within the bucket for the extracted files.
        """
        source_path = Path(source_path)
        target_path = self.local_base_path / bucket_name / bucket_key

        # Ensure target directory exists
        target_path.mkdir(parents=True, exist_ok=True)

        # Get the list of files already in the target directory
        existing_files = set(f.name for f in target_path.glob("*") if f.is_file())

        # Collect all files available in the source zip files
        files_to_extract = set()
        for zip_file in source_path.glob("*.zip"):
            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    files_to_extract.update(zf.namelist())
            except Exception as e:
                print(f"Error reading zip file '{zip_file}': {e}")

        # Remove files that already exist in the target directory
        missing_files = files_to_extract - existing_files
        if not missing_files:
            print("No files to extract.")
            return

        print(f"Files to extract: {missing_files}")

        # Extract missing files
        for zip_file in source_path.glob("*.zip"):
            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    zip_contents = set(zf.namelist())
                    # Find files in this zip that are missing
                    files_to_extract_from_this_zip = missing_files.intersection(zip_contents)

                    for filename in files_to_extract_from_this_zip:
                        zf.extract(filename, target_path)
                        print(f"Extracted '{filename}' from '{zip_file}' to '{target_path}'")
                        missing_files.remove(filename)

                    # If all missing files are found, exit early
                    if not missing_files:
                        break
            except Exception as e:
                print(f"Error extracting files from '{zip_file}': {e}")

        # Log any files that were not found
        if missing_files:
            print(f"The following files were not found in any source zip files: {missing_files}")

    def save_json(self, bucket_name, bucket_key, data):
        """
        Save JSON data directly to the specified bucket.

        Parameters:
            bucket_name (str): Name of the target bucket.
            bucket_key (str): Key (path) within the bucket.
            data (dict): The data to be saved as JSON.
        """
        # Convert the data into a JSON string
        json_data = json.dumps(data)

        # Save the JSON file
        self.upload_file(json_data, bucket_name, bucket_key)
        print(f"Saved JSON data to {bucket_name}/{bucket_key}")

    def folder_exists(self, bucket_name, bucket_key=""):
        """
        Check if a folder exists in the specified bucket.

        Parameters:
            bucket_name (str): Name of the target bucket.
            bucket_key (str): Key (path) within the bucket.
        """
        folder_path = self.local_base_path / bucket_name / bucket_key
        return folder_path.exists()

    def remove_file(self, bucket_name, bucket_key):
        """
        Remove a file from the bucket and local storage.

        Parameters:
            bucket_name (str): Name of the bucket.
            bucket_key (str): Key (path) of the file to remove.
        """
        file_path = self.local_base_path / bucket_name / bucket_key
        if file_path.exists():
            os.remove(file_path)
            print(f"Removed file: {file_path}")
            self.s3.delete_object(Bucket=bucket_name, Key=bucket_key)
            print(f"Removed from mock S3: {bucket_name}/{bucket_key}")
        else:
            print(f"File {file_path} does not exist.")