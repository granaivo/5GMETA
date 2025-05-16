
import requests
import security


platformuser = "5gmeta"
platformpassword = "5Gm3t4!"

headers=security.get_header_with_token(platformuser,platformpassword)


mecid="2"
tile="1222112111"
server="http://5gmeta-platform.eu/discovery-api/"
url = server+'mec/'+mecid+"/tile/"+tile

x = requests.delete(url,headers)

print(x.text)
