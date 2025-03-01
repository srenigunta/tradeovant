import duckdb
import pyarrow.parquet as pq
import os
import yaml
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to list partitions in a directory
def list_partitions_in_dir(base_dir, partition_column="file_version_date"):
    """
    Return a sorted list of partition values found in base_dir as subdirectories like:
    file_version_date=YYYYMMDD
    """
    if not os.path.exists(base_dir):
        logging.warning(f"Directory does not exist: {base_dir}")
        return []
    partitions = []
    for entry in os.listdir(base_dir):
        if entry.startswith(f"{partition_column}="):
            val = entry.split("=")[1]
            partitions.append(val)
    return sorted(partitions)

# Main processing function
def process_datasets(config_file='symbol_master.yaml'):
    # Load YAML config
    logging.info("Loading symbol_master.yaml...")
    current_dir = os.path.dirname(__file__)
    sql_path = os.path.join(current_dir, "..", "config", "symbol_master.yaml")
    sql_config_file = os.path.abspath(sql_path)

    with open(sql_config_file, "r") as f:
        config = yaml.safe_load(f)
    
    datasets = config.get('datasets', {})
    if not datasets:
        logging.error("No datasets found in configuration file.")
        return

    for dataset_name, ds_conf in datasets.items():
        logging.info(f"Processing dataset: {dataset_name}")
        
        if ds_conf['type'] == 'aggregated':
            # Process aggregated dataset
            try:
                # Format SQL with source paths
                sql = ds_conf['sql'].format(**ds_conf['sources'])
                
                # Connect to DuckDB
                conn = duckdb.connect()
                
                # Execute SQL and get result as Arrow table
                result = conn.execute(sql).arrow()
                
                # Ensure target directory exists
                target_dir = os.path.dirname(ds_conf['target'])
                os.makedirs(target_dir, exist_ok=True)
                
                # Write result to target file (overwrite if exists)
                pq.write_table(result, ds_conf['target'], compression='snappy')
                logging.info(f"Wrote aggregated data to {ds_conf['target']}")
                
                # Close connection
                conn.close()
            except Exception as e:
                logging.error(f"Error processing {dataset_name}: {e}")
        
        elif ds_conf['type'] == 'partitioned':
            # Process partitioned dataset
            partition_column = ds_conf.get('partition_column', 'file_version_date')
            source_path = ds_conf['source']
            target_path = ds_conf['target']
            
            # List partitions from source
            partitions = list_partitions_in_dir(source_path, partition_column)
            if not partitions:
                logging.warning(f"No partitions found in {source_path}")
                continue
            
            # for pval in partitions:
            #     try:
            #         # Format SQL with source and partition value
            #         sql = ds_conf['sql'].format(source=source_path, partition_val=pval)
                    
            #         # Connect to DuckDB
            #         conn = duckdb.connect()
                    
            #         # Execute SQL and get result as Arrow table
            #         result = conn.execute(sql).arrow()
                    
            #         # Create partition directory and write result
            #         partition_dir = os.path.join(target_path, f"{partition_column}={pval}")
            #         os.makedirs(partition_dir, exist_ok=True)
            #         out_file = os.path.join(partition_dir, "data.parquet")
                    
            #         # Write result to partitioned file (overwrite if exists)
            #         pq.write_table(result, out_file, compression='snappy')
            #         logging.info(f"Wrote partition {partition_column}={pval} to {out_file}")
                    
            #         # Close connection
            #         conn.close()
            #     except Exception as e:
            #         logging.error(f"Error processing partition {pval} in {dataset_name}: {e}")
        
        else:
            logging.error(f"Unknown dataset type '{ds_conf['type']}' for {dataset_name}")

if __name__ == "__main__":
    process_datasets()