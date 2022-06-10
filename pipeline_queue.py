import pyodbc
import os
import glob
import pandas as pd
from datetime import datetime

class pipeline_queue:
    def __init__(self):
        self.conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=BIGBERTHA;'
                      'Database=stayfit_data_metrics;'
                      'Trusted_Connection=yes;')
        self.cursor = self.conn.cursor()

    #Returns a dataframe of files from selected step
    def GetStageFiles(self, StepToRetrieve):
        StepToRetrieve = StepToRetrieve.lower()
        stages = ["staging", "validate", "transform", "process"]
        data = []
        if StepToRetrieve in stages:
            files = glob.glob("C:/API&Pipeline/{}/*.json".format(StepToRetrieve))
        else:
            print("Not a stage")
            return
        for file in files:
            data.append([file[file.find("\\")+1::1], datetime.now(), StepToRetrieve.upper()])
        df = pd.DataFrame(data)
        df.columns=["FileName", "Datetime", "Status"]
        return df

    #Retrieve dataframe of file list from sql server
    def GetCurrentQueue(self):
        try:   
            query = "SELECT [Data_File_Name], [Created_Timestamp], [File_Status] FROM pipeline_queue;"
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as exception_message:
            print("Queue Get Error:", exception_message)
    
    #Tests to see if all status is = done
    def CheckIfDone(self):
        query = "SELECT [File_Status] FROM pipeline_queue;"
        df = pd.read_sql(query, self.conn)
        i = 0
        for row in df.itertuples():
            if row.File_Status == "DONE":
                i += 1
        
        if len(df) == i:
            return True
        else:
            return False
        

    #Gets next file to feed through pipeline
    def GetNextFile(self):
        query = "Select [Data_File_Name], [File_Status] FROM pipeline_queue;"
        df = pd.read_sql(query, self.conn)
        for row in df.itertuples():
            if row.File_Status == "STAGING":
                return row.Data_File_Name

    #Send dataframe of file list to SQL server
    def AddToQueue(self, df):
        try:
            for row in df.itertuples():
                self.cursor.execute('''
                            INSERT INTO pipeline_queue (Data_File_Name, Created_Timestamp, File_Status)
                            VALUES (?,?,?)
                            ''',
                            row.FileName, 
                            row.Datetime,
                            row.Status,
                            )
            self.conn.commit()
        except Exception as exception_message:
            print("Queue Appending Error:", exception_message)

    #Updates a file's status
    def UpdateQueue(self, Filename, NextStepStatus):
        try:     
            self.cursor.execute('''
                            UPDATE pipeline_queue
                            SET File_Status = '{}'
                            WHERE Data_File_Name = '{}';
                            '''.format(NextStepStatus.upper(), Filename))
            self.conn.commit()
        except Exception as exception_message:
            print("Update Error:", exception_message)
    
    #Clear Queue
    def ClearQueue(self):
        self.cursor.execute('''DELETE FROM pipeline_queue;''')
        self.conn.commit()