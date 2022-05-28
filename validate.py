import json
import glob
import os
import shutil

#Get latest file from staging
list_of_files = glob.glob('C:/API&Pipeline/staging/*.json')
latest_file = max(list_of_files, key=os.path.getctime)
file_name = latest_file[latest_file.find("\\")+1::1]

#Make a copy of file in validate folder
src_path = r"C:\API&Pipeline\staging\{}".format(file_name)
target_path = r"C:\API&Pipeline\validate\{}".format(file_name)
shutil.copyfile(src_path, target_path)
os.remove(src_path)

#Load json data into a dict
f = open(file_name)
data = json.load(f)
f.close()

#Check all fields for wrong/missing values, and append to errors dictionary
#  Error#: [Field Name:, Wrong Value, Entry Number]
errors = {}
is_errors = False
i = 0 #Updated every iteration
count = 0 #Updated whenever an error occurs
expected = [['Americas', "Western Pacific"], ["United States of America", "Canada", "Japan", "China"],
["Male", "Female", "Both sexes"], ["2000", "2005", "2010", "2015", "2018", "2019", "2020", "2023", "2025"]]
for entry in data['fact']:
    if entry["dim"]['REGION'] not in expected[0]:
        errors["Error{}".format(str(count + i))] = ["REGION:", entry["dim"]['REGION'], "Entry {}".format(i)]
        is_errors = False
        count += 1
    if entry["dim"]["COUNTRY"] not in expected[1]:
        errors["Error{}".format(str(count + i))] = ["COUNTRY:", entry["dim"]['COUNTRY'], "Entry {}".format(i)]
        is_errors = False
        count += 1
    if entry["dim"]["SEX"] not in expected[2]:
        errors["Error{}".format(str(count + i))] = ["SEX:", entry["dim"]['SEX'], "Entry {}".format(i)]
        is_errors = False
        count += 1
    if entry["dim"]["YEAR"] not in expected[3]:
        errors["Error{}".format(str(count + i))] = ["YEAR:", entry["dim"]['YEAR'], "Entry {}".format(i)]
        is_errors = False
        count += 1
    is_float = False
    try:
        float(entry["Value"])
        is_float = True
    except ValueError:
        errors["Error{}".format(str(count + i))] = ["VALUE:", entry["Value"], "Entry {}".format(i)]
        is_errors = False
        count += 1
    if is_float == True:
        if float(entry["Value"]) < 0 or float(entry["Value"]) > 100:
            errors["Error{}".format(str(count + i))] = ["VALUE:", entry["Value"], "Entry {}".format(i)]
            is_errors = False
            count += 1
    i += 1

#Get a int list of indexed error entries
error_index = []
for error in errors:
    error_index.append(int(errors[error][2][5::1]))

#Find the occurences of each entry to find duplicates
i = 0
dupe_validation = dict()
for entry in data["fact"]:
    if i not in error_index: #If entry has an error we dont put i in the dict
        country = entry["dim"]["COUNTRY"]
        year = entry["dim"]["YEAR"]
        fullname = country + year
        dupe_validation[fullname] = dupe_validation.get(fullname, 0) + 1
    i += 1

#Find Dupes - val needs to = 3
dupes = {}
for country in dupe_validation:
    if dupe_validation[country] > 3:
        dupes[country] = "Duplicate Entry(ies)"
    if dupe_validation[country] < 3:
        dupes[country] = "Missing Entry(ies)"


print(errors)
print(dupes)