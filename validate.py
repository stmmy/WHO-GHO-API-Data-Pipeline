import json
import glob
import os
import shutil
from datetime import datetime
import pandas as pd
import sys

def validate(file_name):
    #Create log dataframe
    logs = pd.DataFrame(columns = ["File_Name", "Log_Message", "Time_Stamp", "Stage_Of_File"])
    
    
    #Move data file into validate
    try:
        src_path = r"C:\API&Pipeline\staging\{}".format(file_name)
        target_path = r"C:\API&Pipeline\validate\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
    except:
        logs.loc[len(logs.index)] = [file_name, "File transfer into validate failed", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        sys.exit()
    else:
        logs.loc[len(logs.index)] = [file_name, "File transfer into validate successfull", datetime.now(), "Validation"]


    #Load json data into a dict
    try:
        f = open(target_path)
        data = json.load(f)
        f.close()
    except:
        logs.loc[len(logs.index)] = [file_name, "File unable to be loaded", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        sys.exit()
    else:
        logs.loc[len(logs.index)] = [file_name, "File loaded successfull", datetime.now(), "Validation"]
        


    #Check all fields for wrong/missing values, and append to errors dictionary
    #  Error#: [Field Name:, Wrong Value, Entry Number]
    try:
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
            if "[" in entry["Value"]:
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
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in checking data fields", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        sys.exit()
    else:
        logs.loc[len(logs.index)] = [file_name, "Data Fields parsed", datetime.now(), "Validation"]
    

    #Check for dupes/missing entries
    try:
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

        #Find Dupes - val needs to = 3, based off of amt of genders filtered, we have all three - male, female, both
        dupes = dict()
        for country in dupe_validation:
            if dupe_validation[country] > 3:
                dupes[country] = "Duplicate Entry(ies)"
                is_errors = True
            if dupe_validation[country] < 3:
                dupes[country] = "Missing Entry(ies)"
                is_errors = True
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in checking for duplicates/missing entries", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        sys.exit()
    else:
        logs.loc[len(logs.index)] = [file_name, "Finished checking for duplicates/missing entries", datetime.now(), "Validation"]


    #Cleans up based off of if there is an error(s) or not
    if is_errors == True:
        with open(r"C:\API&Pipeline\errors\error_{}.txt".format(file_name[0:file_name.find(".")]), 'w') as f:
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
        logs.loc[len(logs.index)] = [file_name, "Error(s)/Duplicate(s) found - Check C:/API&Pipeline/validate/errors for full error log", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        src_path = r"C:\API&Pipeline\validate\{}".format(file_name)
        target_path = r"C:\API&Pipeline\validate\errors\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
        return False
    else:
        logs.loc[len(logs.index)] = [file_name, "Validated Successfully", datetime.now(), "Validation"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        src_path = r"C:\API&Pipeline\validate\{}".format(file_name)
        target_path = r"C:\API&Pipeline\transform\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
        return True
