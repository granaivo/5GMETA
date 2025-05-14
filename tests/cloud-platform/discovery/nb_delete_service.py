
import requests


import requests

#curl -X DELETE -H "Content-Type: application/json" http://192.168.10.9:8080/v0/mecregistry/1/nbservices/1


mecid="1"
service="1"
server="http://192.168.10.9:8080/v0/"
url = server+'mecregistry/'+mecid+"/nbservices/"+service

x = requests.delete(url)

print(x.text)
