import requests
import json


# curl -X PATCH -H "Content-Type: application/json" -d @nb_update_service_example.json http://192.168.10.9:8080/v0/mecregistry/1/nbservices/1

mecid="1"
serviceid="1"
server="http://192.168.10.9:8080/v0/"
url = server+'mecregistry/'+mecid+"/nbservices/"+serviceid


headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}

x = requests.patch(url, data=open('nb_update_service_example.json', 'rb'),headers=headers)

json_response=x.json()
print(json_response)
