import json
import glob
import os
import shutil
from datetime import datetime
import pandas as pd


#Open most recent file in folder and get data
list_of_files = glob.glob('C:/API&Pipeline/transform/*.json')
latest_file = max(list_of_files, key=os.path.getctime)
file_name = latest_file[latest_file.find("\\")+1::1]
f = open(file_name)
data = json.load(f)
f.close()

#Create dataframe from json file
fdata = []
for entry in data["fact"]:
    country_code = "USA" if entry["dim"]["COUNTRY"] == "United States of America" else entry["dim"]["COUNTRY"][:2].upper()
    fdata.append([entry["dim"]["COUNTRY"],country_code,entry["dim"]["YEAR"],entry["dim"]["SEX"], float(entry["Value"][0:entry["Value"].find("[")-1])])
df = pd.DataFrame(fdata)
df.columns=["Contry", "Country_Code", "Year", "Sex","Value"]
df.to_csv("C:/API&Pipeline/processed/{}_cig_metrics.csv".format(datetime.now().strftime("%Y%m%d%H%M%S")))
