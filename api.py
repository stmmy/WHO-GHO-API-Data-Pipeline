import os
import requests
from datetime import datetime
#WHO GHO Athena API - https://www.who.int/data/gho/info/athena-api

#https://apps.who.int/gho/athena/api/COUNTRY - List of countries
#https://apps.who.int/gho/athena/api/REGION - List of regions
#Get different dimensions - https://apps.who.int/gho/athena/api/GHO
#?profile=simple&filter=COUNTRY:BWA;YEAR:2020;YEAR:2023 - multiple filters
#'filter': 'COUNTRY:USA;COUNTRY:CAN;COUNTRY:CHN;COUNTRY:JPN'

dimension = "M_Est_cig_curr"
params = {'profile': 'simple', 'filter': 'COUNTRY:JPN'}
download_request_url = "https://apps.who.int/gho/athena/api/GHO/" + dimension + ".json"

#Check values
print("Download URl:", download_request_url)
print("Params:", params)
error_status = False

#Test request
try: 
    response = requests.get(download_request_url, params=params, allow_redirects=True, timeout=90)
except Exception as exception_message:
    message = 'requests.get failed -'
    print(message, exception_message)
    error_status = True
    raise Exception(exception_message) 
    
#Test status code of request
try: 
    status_code = response.status_code
    print("Status Code:", status_code)
except Exception as exception_message:
    message = "request.status_code failed -"
    print(message, exception_message)
    error_status = True
    raise Exception(exception_message)

if status_code < 200 or status_code > 299: #200-299 are successful HTTP response codes
    error_status = True

#Format Params for File name
formatted_params = ""
read = False
for i in range(len(params["filter"])):
    if read == True:
        formatted_params += params['filter'][i]
    if params['filter'][i] == ":":
        read = True
    if params['filter'][i] == ";":
        read = False

#Write to file
if error_status != True:
    fname = os.path.join("C:/API&Pipeline/staging", "{}_{}.json".format(datetime.now().strftime("%Y%m%d"), formatted_params))
    data = response.content
    with open(fname, 'wb') as f:
            f.write(data)
