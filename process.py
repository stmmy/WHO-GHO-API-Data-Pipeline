import pandas as pd
import pyodbc
import glob
import os
import shutil

import sys

def process(file_name):
    #Read csv data into a dataframe
    file_path = r"C:\API&Pipeline\process\{}".format(file_name)
    df = pd.read_csv(file_path, index_col=0)

    #Connection to sql server
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=BIGBERTHA;'
                        'Database=stayfit_data_metrics;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    error = False

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
        print(exception_message, "\nData moved to errors.")
        error = True
        src_path = r"C:\API&Pipeline\process\{}".format(file_name)
        target_path = r"C:\API&Pipeline\errors\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)

    if error != True:
        print("Data transferred successfully. Data archived.")
        src_path = r"C:\API&Pipeline\process\{}".format(file_name)
        target_path = r"C:\API&Pipeline\archived\{}".format(file_name)
        shutil.copyfile(src_path, target_path)
        os.remove(src_path)