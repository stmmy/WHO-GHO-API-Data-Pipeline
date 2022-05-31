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

#Sort data
fdata = []
for entry in data["fact"]:
   fdata.append([entry["dim"]["COUNTRY"], entry["dim"]["YEAR"], entry["dim"]["SEX"], float(entry["Value"][0:entry["Value"].find("[")-1])])
sdata = sorted(fdata)


#Get lists for pandas dataframe
mapping = {"United States of America": "USA", "Canada": "CAN", "Japan": "JPN", "China": "CHN"} #THIS CODE NEEDS TO BE CHANGED FOR DIFF FILTERS
country = []
country_code = []
year = []
value_both = []
value_female = []
value_male = []

num_of_years = 0
for i in range(len(sdata)):
    if sdata[i][1] not in year:
        year.append(sdata[i][1])
        num_of_years += 1

num_of_countries = 0
for i in range(len(sdata)):
    if sdata[i][0] not in country:
        for j in range(num_of_years):
            country.append(sdata[i][0])
            country_code.append(mapping[sdata[i][0]])
        num_of_countries += 1
    if sdata[i][2] == "Both sexes":
        value_both.append(float(sdata[i][3]))
    elif sdata[i][2] == "Female":
        value_female.append(float(sdata[i][3]))
    elif sdata[i][2] == "Male":
        value_male.append(float(sdata[i][3]))
year = year*num_of_countries 

#Create Dataframe
df = pd.DataFrame()
df["Country"] = country
df["CC"] = country_code
df["Year"] = year
df["Both Sexes"] = value_both
df["Female"] = value_female
df["Male"] = value_male