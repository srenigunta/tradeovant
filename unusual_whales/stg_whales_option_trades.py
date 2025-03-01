import os
import glob
import logging
import shutil
import yaml
import duckdb
import pyarrow.parquet as pq
from tradeovant.imports.common_utils import LocalS3WithDirectory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def list_partitions_in_dir(base_dir, partition_column="file_version_date"):
    """
    Return a sorted list of partition values found in `base_dir` as subdirectories like:
      file_version_date=YYYYMMDD
    """
    if not os.path.exists(base_dir):
        return []
    partitions = []
    for entry in os.listdir(base_dir):
        # Expect subdirs named like "file_version_date=20250101"
        if entry.startswith(f"{partition_column}="):
            val = entry.split("=")[1]
            partitions.append(val)
    return sorted(partitions)


def remove_partition_dir(base_dir, partition_val, partition_column="file_version_date"):
    """
    Remove an existing partition directory if it exists, e.g.:
      /base_dir/file_version_date=YYYYMMDD
    """
    part_dir = os.path.join(base_dir, f"{partition_column}={partition_val}")
    if os.path.exists(part_dir):
        shutil.rmtree(part_dir)
        logging.info(f"Removed existing partition directory: {part_dir}")


def transform_partition_parquet(
        source_path,
        target_path,
        partition_val,
        partition_column,
        pre_select,
        pre_grouping,
        core_select,
        core_grouping,
        dataset_join,
        file_name
):
    """
    1) Read a single partition (partition_val) from `source_path`.
    2) Load it into DuckDB as 'input_data'.
    3) Run the query_select to produce a result.
    4) Write the result to `target_path` as a partitioned Parquet.
    """

    # Partition directory for reading
    source_partition_dir = os.path.join(source_path, f"{partition_column}={partition_val}")
    if not os.path.exists(source_partition_dir):
        logging.warning(f"Source partition dir does not exist: {source_partition_dir}")
        return

    # Gather all parquet files within the partition folder
    parquet_files = glob.glob(os.path.join(source_partition_dir, "*.parquet"))
    if not parquet_files:
        logging.warning(f"No parquet files found in {source_partition_dir}")
        return

    # Connect DuckDB in-memory
    conn = duckdb.connect()
    raw_path = rf"{source_path.replace("\\", "/")}/*/*.parquet"
    presql = f"""
        CREATE TEMP TABLE temp_options_flow AS
        SELECT *
        FROM read_parquet("{raw_path}", hive_partitioning = TRUE)
        WHERE file_version_date = '{partition_val}';
        """
    #print(f"presql is: {presql}" )
    conn.execute(presql)

    logging.info(f"Processing.. {partition_column}={partition_val}")
    # Execute the custom SQL
    #arrow_table = conn.execute(query_select).arrow()
    sql_stmt = f"""{pre_select} from temp_options_flow
                    {pre_grouping} 
                    {core_select}  from temp_options_flow
                    {dataset_join} 
                    {core_grouping}"""
    #print(f"sql statement is : {sql_stmt}")
    arrow_table = conn.execute(sql_stmt).arrow()

    # Write the result to the target partition path
    target_partition_dir = os.path.join(target_path, f"{partition_column}={partition_val}")
    os.makedirs(target_partition_dir, exist_ok=True)
    out_file = os.path.join(target_partition_dir, f"stg_{file_name}.parquet")

    pq.write_table(
        table=arrow_table,
        where=out_file,
        compression="snappy"
    )
    logging.info(f"Wrote transformed data to {out_file}")

    # Cleanup
    conn.close()


def main(rerun_partitions):
    """
    Main routine to:
      1) Initialize local storage
      2) Load config
      3) For each dataset:
         - Build full stage + bronze paths
         - List source partitions
         - Skip or rerun
         - Transform with DuckDB
      4) Cleanup
    """

    # 1) Initialize Local S3-like storage
    storage = LocalS3WithDirectory()
    logging.info("Initialized local storage with base path: %s", storage.BASE_PATH)

    # 2) Load the YAML
    os.environ["NUMEXPR_MAX_THREADS"] = "16"
    logging.info("Loading whales_sql_option_trades.yaml...")
    current_dir = os.path.dirname(__file__)
    sql_path = os.path.join(current_dir, "..", "config", "whales_sql_option_trades.yaml")
    sql_config_file = os.path.abspath(sql_path)

    with open(sql_config_file, "r") as f:
        config = yaml.safe_load(f)
    datasets = config.get("datasets", {})

    # 3) Process each dataset
    for dataset_name, ds_conf in datasets.items():
        logging.info(f"Processing dataset: {dataset_name}")

        # The relative paths from config
        raw_sub_path = ds_conf["raw_sub_path"]
        stage_sub_path = ds_conf["stage_sub_path"]
        partition_column = ds_conf.get("partition_column", "file_version_date")
        pre_select = ds_conf["pre_select"]
        pre_grouping = ds_conf["pre_grouping"]
        core_select = ds_conf["core_select"]
        core_grouping = ds_conf["core_grouping"]
        dataset_join = ds_conf["dataset_join"]
        file_name = dataset_name

        # Build the actual absolute paths by joining with storage.BASE_PATH
        raw_path = os.path.join(storage.BASE_PATH, raw_sub_path)
        stage_path = os.path.join(storage.BASE_PATH, stage_sub_path)

        logging.info(f"Raw path: {raw_path}")
        logging.info(f"Stage path: {stage_path}")

        # List partitions in source (stage) and target (bronze)
        source_partitions = list_partitions_in_dir(raw_path, partition_column)
        target_partitions = list_partitions_in_dir(stage_path, partition_column)

        logging.info(f"Found {len(source_partitions)-len(target_partitions)} partitions in raw for {dataset_name} to be processed...")

        # Iterate over each partition in the stage data
        for pval in source_partitions:
            do_rerun = (rerun_partitions is not None) and (pval in rerun_partitions)
            already_in_target = (pval in target_partitions)

            # Decide if we should process this partition
            if do_rerun:
                logging.info(f"Re-run requested for partition={pval}. Removing old data in target.")
                remove_partition_dir(stage_path, pval, partition_column)

            if (not already_in_target) or do_rerun:
                transform_partition_parquet(
                    source_path=raw_path,
                    target_path=stage_path,
                    partition_val=pval,
                    partition_column=partition_column,
                    pre_select = pre_select,
                    pre_grouping = pre_grouping,
                    core_select = core_select,
                    core_grouping = core_grouping,
                    dataset_join = dataset_join,
                    file_name=file_name
                )

        logging.info(f"Done processing dataset: {dataset_name}")

    # 4) Done - if you have any final storage cleanup logic, do it here
    storage.stop()
    logging.info("All done. Storage stopped.")


if __name__ == "__main__":
    rerun_partitions = None # Add list of file_version_dates such as ['20250122', '20250123']
    main(rerun_partitions)
