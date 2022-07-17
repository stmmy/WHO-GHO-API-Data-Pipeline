import pyodbc
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=BIGBERTHA;'
                        'Database=stayfit_data_metrics;'
                        'Trusted_Connection=yes;')

years = ["2025", "2023", "2020", "2019", "2018", "2015", "2010", "2005", "2000"]

for year in years:
    #male
    query = """SELECT [country_name], [value] from cigarette_metrics WHERE year = {} AND sex = 'Male'""".format(year)
    df = pd.read_sql_query(query, con=conn)
    ax = df.plot(kind='bar', x='country_name', y='value', title="Male Smoking Percentage of Population {}".format(year))
    ax.set_xlabel("Countries")
    ax.set_ylabel("Percentage")

    #female
    query = """SELECT [country_name], [value] from cigarette_metrics WHERE year = {} AND sex = 'Female'""".format(year)
    df = pd.read_sql_query(query, con=conn)
    ax = df.plot(kind='bar', x='country_name', y='value', title="Female Smoking Percentage of Population {}".format(year))
    ax.set_xlabel("Countries")
    ax.set_ylabel("Percentage")

    #both
    query = """SELECT [country_name], [value] from cigarette_metrics WHERE year = {} AND sex = 'Both sexes'""".format(year)
    df = pd.read_sql_query(query, con=conn)
    ax = df.plot(kind='bar', x='country_name', y='value', title="Female And Male Smoking Percentage of Population {}".format(year))
    ax.set_xlabel("Countries")
    ax.set_ylabel("Percentage")
