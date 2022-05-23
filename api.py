import requests
import json
#WHO GHO Athen API - https://www.who.int/data/gho/info/athena-api



#Get different dimensions - https://apps.who.int/gho/athena/api/GHO
dimension = "M_Est_tob_curr_std"
params = {'profile': 'simple'}
download_request_url = "https://apps.who.int/gho/athena/api/GHO/" + dimension + ".json"

#Check values
print("Download URl:", download_request_url)
print("Params:", params)


try: #Test request
    response = requests.get(download_request_url, params=params, allow_redirects=True, timeout=90)
except Exception as exception_message:
    message = 'requests.get failed -'
    print(message, exception_message)
    error_status = True

try: #Test status code of request
    status_code = response.status_code
    print("Status Code:", status_code)
except Exception as exception_message:
    message = "request.status_code failed -"
    print(message, exception_message)
    erro_status = True






print(response.status_code)
#data = jdata.content
#with open('data.json', 'wb') as f:
#        f.write(data)