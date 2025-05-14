import requests
import json
import security


platformuser = "5gmeta"
platformpassword = "5Gm3t4!"

headers=security.get_header_with_token(platformuser,platformpassword)




server="http://5gmeta-platform.eu/discovery-api/"
tile="033111012102302012"
url = server+'mec'

path=url+"/tile/"+tile

r = requests.get(path,headers)
json_response=r.json()

print(json_response)
#if (len(json_response) == 1):
#    print("MEC ID where tile "+tile+" belong is :" + str(json_response[0]['id']))
#else:
#    print("Tile "+tile+" not found")
