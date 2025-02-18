import json
import pandas as pd


json_path = "R:/local_bucket/raw_store/stockcharts/sctr_reports/largecap_sctr_report/json/largecap_sctr_report_20250212.json"
json_path2 = "R:/local_bucket/raw_store/stockcharts/sctr_reports/midcap_sctr_report/json/midcap_sctr_report_20250212.json"
json_path3 = "R:/local_bucket/raw_store/stockcharts/sctr_reports/smallcap_sctr_report/json/smallcap_sctr_report_20250212.json"
json_path4 = "R:/local_bucket/raw_store/stockcharts/sctr_reports/etf_sctr_report/json/etf_sctr_report_20250212.json"

# Open and read the JSON file
with open(json_path, 'r') as file:
    data = json.load(file)
df = pd.DataFrame(data)
df.info()

# Open and read the JSON file
with open(json_path2, 'r') as file:
    data2 = json.load(file)
df2 = pd.DataFrame(data2)
df2.info()

# Open and read the JSON file
with open(json_path3, 'r') as file:
    data3 = json.load(file)
df3 = pd.DataFrame(data3)
df3.info()

# Open and read the JSON file
with open(json_path4, 'r') as file:
    data4 = json.load(file)
df4 = pd.DataFrame(data4)
df4.info()
