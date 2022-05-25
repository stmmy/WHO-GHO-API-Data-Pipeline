import os
import requests
import datetime
import calendar
#WHO GHO Athen API - https://www.who.int/data/gho/info/athena-api

#https://apps.who.int/gho/athena/api/COUNTRY - List of countries
#https://apps.who.int/gho/athena/api/REGION - List of regions
#?profile=simple&filter=COUNTRY:BWA;YEAR:2020;YEAR:2023 - multiple filters


#Get different dimensions - https://apps.who.int/gho/athena/api/GHO
dimension = "M_Est_tob_curr_std"
params = {'profile': 'simple', 'filter': 'COUNTRY:USA'}
download_request_url = "https://apps.who.int/gho/athena/api/GHO/" + dimension + ".json"

#Check values
print("Download URl:", download_request_url)
print("Params:", params)
error_status = False


try: #Test request
    response = requests.get(download_request_url, params=params, allow_redirects=True, timeout=90)
except Exception as exception_message:
    message = 'requests.get failed -'
    print(message, exception_message)
    error_status = True
    raise Exception(exception_message) 
    

try: #Test status code of request
    status_code = response.status_code
    print("Status Code:", status_code)
except Exception as exception_message:
    message = "request.status_code failed -"
    print(message, exception_message)
    error_status = True
    raise Exception(exception_message)

if status_code < 200 or status_code > 299: #200-299 are successful HTTP response codes
    error_status = True

#Write to file
if error_status != True:
    fname = os.path.join("C:/API&Pipeline/staging", "datapull-{}.json".format(calendar.timegm(datetime.datetime.utcnow().utctimetuple())))
    data = response.content
    with open(fname, 'wb') as f:
            f.write(data)
