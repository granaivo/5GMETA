import requests
import json

# curl -X GET -H "Content-Type: application/json" http://192.168.10.9:8080/v0//v0/mecregistry/<mec_id>/nbservices

mecid="1"
server="http://192.168.10.9:8080/v0/"
url = server+'mecregistry/'+mecid+"/nbservices"

r = requests.get(url)
json_response=r.json()
print(json_response)
