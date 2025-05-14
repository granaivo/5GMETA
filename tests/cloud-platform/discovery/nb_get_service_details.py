import requests
import json


mecid="1"
serviceid="1"
server="http://192.168.10.9:8080/v0/"
url = server+'mecregistry/'+mecid+"/nbservices/"+serviceid

r = requests.get(url)
json_response=r.json()
print(json_response)
