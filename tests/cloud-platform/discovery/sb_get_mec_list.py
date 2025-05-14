import requests
import json
import security


platformuser = "5gmeta"
platformpassword = "5Gm3t4!"

headers=security.get_header_with_token(platformuser,platformpassword)


server="https://5gmeta-platform.eu/discovery-api/"
url = server+'mec'

print(url)
r = requests.get(url,headers=headers)
print(r.headers)
json_response=r.json()

print(json_response)
