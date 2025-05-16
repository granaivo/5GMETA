import requests
import json

#curl -X POST -H "Content-Type: application/json" -d @sb_register_example.json http://5gmeta-platform.eu/discovery-api/mec
 

#server="http://5gmeta-platform.eu/discovery-api/"
server="http://192.168.10.9:8080/"
url = server+'mec'


headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}

x = requests.post(url, data=open('sb_register_example.json', 'rb'),headers=headers)

json_response=x.json()
#print("Given MEC id is: ", json_response['mec_id'])
print(json_response)
