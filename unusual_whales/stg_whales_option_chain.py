import duckdb
import pyarrow.parquet as pq
import os
import yaml
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def list_partitions(base_dir, partition_column="file_version_date"):
    """List partition values (e.g., '20250228') from directories like 'file_version_date=20250228'."""
    if not os.path.exists(base_dir):
        logging.warning(f"Directory does not exist: {base_dir}")
        return []
    partitions = []
    for entry in os.listdir(base_dir):
        if entry.startswith(f"{partition_column}="):
            partitions.append(entry.split("=")[1])
    return sorted(partitions)

def process_partitions(config_file='whales_stg_option_chains.yaml', rerun_partitions = None):
    """Process partitioned datasets based on YAML config."""
    # Load YAML configuration
    logging.info("Loading whales_stg_option_chains.yaml...")
    current_dir = os.path.dirname(__file__)
    sql_path = os.path.join(current_dir, "..", "config", config_file)
    sql_config_file = os.path.abspath(sql_path)

    with open(sql_config_file, "r") as f:
        config = yaml.safe_load(f)

    datasets = config.get('datasets', {})
    if not datasets:
        logging.error("No datasets found in configuration.")
        return

    for dataset_name, ds_config in datasets.items():
        if ds_config.get('type') != 'partitioned':
            logging.info(f"Skipping non-partitioned dataset: {dataset_name}")
            continue

        logging.info(f"Processing dataset: {dataset_name}")

        partition_column = ds_config.get('partition_column', 'file_version_date')
        target_base = ds_config['target']
        # Use the first source path to list partitions (assumes consistent partitioning)
        source_base = list(ds_config['sources'].values())[0].split('*')[0].rstrip('\\')

        # Get source and target partitions
        source_partitions = list_partitions(source_base, partition_column)
        target_partitions = list_partitions(target_base, partition_column)

        # Determine partitions to process
        if rerun_partitions:
            partitions_to_process = [p for p in source_partitions if p in rerun_partitions]
        else:
            partitions_to_process = [p for p in source_partitions if p not in target_partitions]

        logging.info(f"Partitions to process: {len(partitions_to_process)}")

        for partition_val in partitions_to_process:
            try:
                # Format SQL with source paths and partition value
                sql = ds_config['sql'].format(**ds_config['sources'], partition_val=partition_val)

                # Execute SQL with DuckDB
                conn = duckdb.connect()
                result = conn.execute(sql).arrow()
                conn.close()

                # Write to target partition
                target_dir = os.path.join(target_base, f"{partition_column}={partition_val}")
                os.makedirs(target_dir, exist_ok=True)
                output_file = os.path.join(target_dir, "optionchains.parquet")
                pq.write_table(result, output_file, compression='gzip', row_group_size=1000)

                logging.info(f"Processed partition {partition_val} to {output_file}")
            except Exception as e:
                logging.error(f"Error processing partition {partition_val}: {e}")

if __name__ == "__main__":
    # Optionally specify partitions to rerun (e.g., ['20250228'])
    rerun_partitions = None  # Set to a list if you want to rerun specific partitions
    process_partitions(rerun_partitions=rerun_partitions)