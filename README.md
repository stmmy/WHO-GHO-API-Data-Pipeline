# WHO-GHO-API-Data-Pipeline


This is a program to collect and extract data from the WHO Global Health Observatory for statistical use. This repository only gets the data via a API and feeds it through a pipline which validates it's fields, and transforms the pertinent data into pandas dataframe which sends it to a SQL Server for storage.

The API portion of this program interfaces with the WHO GHO Athen API. Documentation on it can be found here https://www.who.int/data/gho/info/athena-api. No API key is needed as WHO is global health institution. The api.py script provided can be used to retrieve any data from the Global Health Observatory so long as you find what topic of data you want here - https://www.who.int/data/gho/data/indicators - and reference it it to the dimension codes found here - https://apps.who.int/gho/athena/api/GHO. For example this current api is set up to find the Estimates of Smoking populations around the world. By searching for smoking estimates in the dimension code list, we get the code "M_Est_cig_curr", which is what this api uses.

The pipeline scripts will need to be modified slightly or heavily, depending on the data. As previously stated, this pipeline is specifically tailored for data of smoking estimates around the globe. Even more specifically, it is filtered for just four countries. The validate script validates all fields and, depending on the signifigance of errors, sends it forward to the transform, folder or into a errors folder with a error log. The transform script transforms the pertinent validated fields into a pandas dataframe where it will then try to insert that data to a sql server. Depending on the outcome of a successfull transferance, the remaining data will be archived or put into an errors table.

Folder Structure : 

*---Main

*------api

*---------api.py

*------errors

*------process

*------staging

*------testing

*------transform

*------validate

*------(rest of python scripts and logs.csv)


Call the api, change the code to connect to your sql server, and run the pipeline controller
