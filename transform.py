import json
import os
from datetime import datetime
import pandas as pd
import sys

def transform(file_name):
    #Create log dataframe
    logs = pd.DataFrame(columns = ["File_Name", "Log_Message", "Time_Stamp", "Stage_Of_File"])

    #Open and load data
    try:
        file_path = r"C:\API&Pipeline\transform\{}".format(file_name)
        f = open(file_path)
        data = json.load(f)
        f.close()
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in opening and loading data during transformation", datetime.now(), "Transformation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        return False
    else:
        logs.loc[len(logs.index)] = [file_name, "File transferred and loaded into transformation", datetime.now(), "Transformation"]


    #Create dataframe from json file
    try:
        fdata = []
        for entry in data["fact"]:
            country_code = "USA" if entry["dim"]["COUNTRY"] == "United States of America" else entry["dim"]["COUNTRY"][:2].upper()
            value = entry["Value"][0:entry["Value"].find("[")-1] if "[" in entry["Value"] else entry["Value"]
            fdata.append([entry["dim"]["COUNTRY"], country_code, entry["dim"]["YEAR"], entry["dim"]["SEX"], float(value)])
        df = pd.DataFrame(fdata)
        df.columns=["Country", "Country_Code", "Year", "Sex","Value"]
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in transforming data into newly formatted dataframe", datetime.now(), "Transformation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        return False
    else:
        logs.loc[len(logs.index)] = [file_name, "Data transformed successfully", datetime.now(), "Transformation"]
    
    
    #Export formatted data
    try:
        df.to_csv("C:/API&Pipeline/process/{}.csv".format(file_name[0:file_name.find(".")]))
        os.remove(file_path)
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in writing out data/deleting old files", datetime.now(), "Transformation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        return False
    else:
        logs.loc[len(logs.index)] = [file_name, "Data outputted in csv, old files deleted", datetime.now(), "Transformation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        return True
