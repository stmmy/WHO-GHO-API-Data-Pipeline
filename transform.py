import json
import glob
import os
from datetime import datetime
import pandas as pd

def transform(file_name):
    #Open and load data
    file_path = r"C:\API&Pipeline\transform\{}".format(file_name)
    f = open(file_path)
    data = json.load(f)
    f.close()

    #Create dataframe from json file
    fdata = []
    for entry in data["fact"]:
        country_code = "USA" if entry["dim"]["COUNTRY"] == "United States of America" else entry["dim"]["COUNTRY"][:2].upper()
        fdata.append([entry["dim"]["COUNTRY"], country_code, entry["dim"]["YEAR"], entry["dim"]["SEX"], float(entry["Value"][0:entry["Value"].find("[")-1])])
    df = pd.DataFrame(fdata)
    df.columns=["Country", "Country_Code", "Year", "Sex","Value"]
    df.to_csv("C:/API&Pipeline/process/{}.csv".format(file_name[0:file_name.find(".")]))
    os.remove(file_path)
