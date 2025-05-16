import requests
import json

mecid="1"
server="http://192.168.10.9:8080/v0/"
url = server+'mecregistry/'+mecid+"/nbservices"


headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}

x = requests.post(url, data=open('nb_register_service_example.json', 'rb'),headers=headers)

json_response=x.json()
print(json_response)
