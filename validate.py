import json
import glob
import os
import shutil
from datetime import datetime

def validate(file_name):
    #Move data file in validate
    src_path = r"C:\API&Pipeline\staging\{}".format(file_name)
    target_path = r"C:\API&Pipeline\validate\{}".format(file_name)
    shutil.copyfile(src_path, target_path)
    os.remove(src_path)

    #Load json data into a dict
    f = open(target_path)
    data = json.load(f)
    f.close()

    #Check all fields for wrong/missing values, and append to errors dictionary
    #  Error#: [Field Name:, Wrong Value, Entry Number]
    errors = {}
    is_errors = False
    i = 0 #Updated every iteration
    count = 0 #Updated whenever an error occurs
    expected = [['Americas', "Western Pacific"], ["United States of America", "Canada", "Japan", "China"],         #THIS CODE NEEDS TO BE CHANGED FOR DIFF FILTERS
    ["Male", "Female", "Both sexes"], ["2000", "2005", "2010", "2015", "2018", "2019", "2020", "2023", "2025"]]    #THIS CODE NEEDS TO BE CHANGED FOR DIFF FILTERS
    for entry in data['fact']:
        if entry["dim"]['REGION'] not in expected[0]:
            errors["Error{}".format(str(count + i))] = ["REGION:", entry["dim"]['REGION'], "Entry {}".format(i)]
            is_errors = True
            count += 1
        if entry["dim"]["COUNTRY"] not in expected[1]:
            errors["Error{}".format(str(count + i))] = ["COUNTRY:", entry["dim"]['COUNTRY'], "Entry {}".format(i)]
            is_errors = True
            count += 1
        if entry["dim"]["SEX"] not in expected[2]:
            errors["Error{}".format(str(count + i))] = ["SEX:", entry["dim"]['SEX'], "Entry {}".format(i)]
            is_errors = True
            count += 1
        if entry["dim"]["YEAR"] not in expected[3]:
            errors["Error{}".format(str(count + i))] = ["YEAR:", entry["dim"]['YEAR'], "Entry {}".format(i)]
            is_errors = True
            count += 1
        is_float = False
        new_val = entry["Value"][0:entry["Value"].find("[")-1]
        entry["Value"] = new_val
        try:
            float(entry["Value"])
            is_float = True
        except ValueError:
            errors["Error{}".format(str(count + i))] = ["VALUE:", entry["Value"], "Entry {}".format(i)]
            is_errors = True
            count += 1
        if is_float == True:
            if float(entry["Value"]) < 0 or float(entry["Value"]) > 100:
                errors["Error{}".format(str(count + i))] = ["VALUE:", entry["Value"], "Entry {}".format(i)]
                is_errors = True
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
    dupes = dict()
    for country in dupe_validation:
        if dupe_validation[country] > 3:
            dupes[country] = "Duplicate Entry(ies)"
            is_errors = True
        if dupe_validation[country] < 3:
            dupes[country] = "Missing Entry(ies)"
            is_errors = True

    if is_errors == True:
        print("Validation unsuccessful - Check errors folder.")
        with open(r"C:\API&Pipeline\validate\errors\error_{}.txt".format(file_name), 'w') as f:
            if len(errors) > 0:
                f.write("Errors:\n\n")
                for entry_key in errors.keys():
                    f.write("\t" + entry_key + ": ")
                    f.write("\t" + str(errors[entry_key]))
                    f.write("\n")
                f.write("\n\n")
            if len(dupes) > 0:
                f.write("Duplicates/Missing Entries:\n\n")
                for entry_key in dupes.keys():
                    f.write("\t" + entry_key + ": ")
                    f.write("\t" + str(dupes[entry_key]))
                    f.write("\n")
        src_path = r"C:\API&Pipeline\validate\{}".format(file_name)
        target_path = r"C:\API&Pipeline\validate\errors\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
    else:
        print("Validated successfully")
        src_path = r"C:\API&Pipeline\validate\{}".format(file_name)
        target_path = r"C:\API&Pipeline\transform\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
