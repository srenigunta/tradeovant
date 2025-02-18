from tradeovant.imports.common_utils import LocalS3WithDirectory
import duckdb,os

# Initialize DuckDB connection
con = duckdb.connect()

# Create storage object
storage = LocalS3WithDirectory()

# Construct the local path
stg_fvf_path = os.path.join(storage.BASE_PATH,"stage_store","finviz","finviz_fundamentals_data")
# Get the list of directories in the base path
directories = [
    d for d in os.listdir(stg_fvf_path)
    if os.path.isdir(os.path.join(stg_fvf_path, d)) and d.startswith("file_version_date=")
]

latest_date = max(int(d.split("=")[1]) for d in directories)
stg_fvf_path = f"""{stg_fvf_path}/file_version_date={latest_date}"""

# Create the SQL statement for creating a view.
# Note the quotes around the path in the read_parquet function.
stmt = f"""
CREATE VIEW fv_fund_data AS
SELECT *
FROM read_parquet('{stg_fvf_path}/*.parquet', hive_partitioning = TRUE)
WHERE file_version_date = (
  SELECT MAX(file_version_date)
  FROM read_parquet('{stg_fvf_path}/*.parquet', hive_partitioning = TRUE)
)
"""

# Execute the statement
con.execute(stmt)

# Verify the data
result = con.execute("""
    SELECT file_version_date, COUNT(*) 
    FROM fv_fund_data 
    GROUP BY file_version_date
""").fetchall()

print(result)

storage.stop()