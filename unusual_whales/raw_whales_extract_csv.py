from tradeovant.imports.common_utils import LocalS3WithDirectory

if __name__ == "__main__":
    # Initialize the storage system
    storage = LocalS3WithDirectory()

    # List of source directories
    source_dirs = ["darkpool", "hotchains", "oichanges", "optionsflow", "stockscreener"]

    # Iterate through the directories
    for source_dir in source_dirs:
        # Define the source path dynamically
        source_path = rf"R:\daily_options_history\{source_dir}"

        # Define the target bucket and key dynamically
        target_bucket = "raw_store"
        target_key = f"whales\\{source_dir}\\csv"

        # Fetch and extract files for the current directory
        print(f"Processing: {source_path} -> {target_bucket}/{target_key}")
        storage.fetch_and_extract(source_path, target_bucket, target_key)

    # Stop the service
    storage.stop()
