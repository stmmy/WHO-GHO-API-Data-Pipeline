import pyodbc
import glob
import pandas as pd
from datetime import datetime

class pipeline_queue:
    #Connection to the database
    def __init__(self):
        self.conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=BIGBERTHA;'
                      'Database=stayfit_data_metrics;'
                      'Trusted_Connection=yes;')
        self.cursor = self.conn.cursor()
    
    ###################
    ### QUEUE FUNCTIONS

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
            if row.File_Status == "DONE" or row.File_Status == "FAILED":
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
    
    #################
    ### LOG FUNCTIONS

    def GetLogs(self):
        try:   
            query = "SELECT [File_Name], [Log_Message], [Stage_Of_File] FROM log_data;"
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as exception_message:
            print("Log Get Error:", exception_message)

    #Updates the SQL log table with current log file logs
    def UpdateLogs(self):
        file_path = r"C:\API&Pipeline\logs.csv"
        df = pd.read_csv(file_path, parse_dates=["Time_Stamp"], infer_datetime_format=True)
        try:
            for row in df.itertuples():
                self.cursor.execute('''
                        INSERT INTO log_data (File_Name, Log_Message, Time_Stamp, Stage_Of_File)
                        VALUES (?,?,?,?)
                        ''',
                        row.File_Name,
                        row.Log_Message,
                        row.Time_Stamp,
                        row.Stage_Of_File,
                        )
                self.conn.commit()
        except Exception as exception_message:
            print("Log Update Error:", exception_message)

    #Clears SQL log table and resets local log file
    def ClearLogs(self):
        self.cursor.execute('''DELETE FROM log_data;''')
        self.conn.commit()
        with open("logs.csv", 'r+') as f:
            f.readline()
            f.truncate(f.tell())
    
    #Clears local logs, not sql table
    def ClearLocalLogs(self):
        with open("logs.csv", 'r+') as f:
            f.readline()
            f.truncate(f.tell())

    #####################
    ### CIGDATA FUNCTIONS
    def ClearCigData(self):
        self.cursor.execute("""DELETE FROM cigarette_metrics;""")
        self.conn.commit()
    
    def GetCigData(self):
        try:   
            query = "SELECT [country_name], [country_code], [year], [sex], [value] FROM cigarette_metrics;"
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as exception_message:
            print("Log Get Error:", exception_message)
