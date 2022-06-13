import pandas as pd
import pyodbc
from datetime import datetime
import os
import shutil

def process(file_name):
    #Create log dataframe
    logs = pd.DataFrame(columns = ["File_Name", "Log_Message", "Time_Stamp", "Stage_Of_File"])


    #Read csv data into a dataframe
    try:
        file_path = r"C:\API&Pipeline\process\{}".format(file_name)
        df = pd.read_csv(file_path, index_col=0)

        #Connection to sql server
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=BIGBERTHA;'
                            'Database=stayfit_data_metrics;'
                            'Trusted_Connection=yes;')
        cursor = conn.cursor()
    except:
        logs.loc[len(logs.index)] = [file_name, "Error in reading data and connecting to SQL server", datetime.now(), "Processing"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
    else:
        logs.loc[len(logs.index)] = [file_name, "Data read, connected to SQL server", datetime.now(), "Processing"]


    #Insert pd Dataframe int the created table
    try:
        for row in df.itertuples():
            cursor.execute('''
                        INSERT INTO cigarette_metrics (country_name, country_code, year, sex, value)
                        VALUES (?,?,?,?,?)
                        ''',
                        row.Country,
                        row.Country_Code,
                        row.Year,
                        row.Sex,
                        row.Value
                        )
        conn.commit()
    except Exception as exception_message:
        logs.loc[len(logs.index)] = [file_name, "Error in transferring finished data to SQL server - moved to local errors folder", datetime.now(), "Processing"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        src_path = r"C:\API&Pipeline\process\{}".format(file_name)
        target_path = r"C:\API&Pipeline\errors\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
        return False
    else:
        logs.loc[len(logs.index)] = [file_name, "Data transferred to SQL server successfully - data archived", datetime.now(), "Processing"]
        logs.to_csv('logs.csv', mode='a', index=False, header=False)
        src_path = r"C:\API&Pipeline\process\{}".format(file_name)
        target_path = r"C:\API&Pipeline\archived\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)
        return True
