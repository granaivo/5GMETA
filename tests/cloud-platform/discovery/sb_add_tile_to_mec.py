import requests
import security


platformuser = "5gmeta"
platformpassword = "5Gm3t4!"

headers=security.get_header_with_token(platformuser,platformpassword)


# curl -X POST -H "Content-Type: application/json" http://5gmeta-platform.eu/discovery-api/mec/2/tile/1222112111


mecid="2"
tile="1222112111"
server="http://5gmeta-platform.eu/discovery-api/"
url = server+'mec/'+mecid+"/tile/"+tile

x = requests.post(url,headers)

json_response=x.json()
print(json_response)
#print("Given MEC id is: ", json_response['mec_id'])
