import unittest
import requests

#curl -X DELETE -H "Content-Type: application/json" http://192.168.10.9:8080/v0/mecregistry/1/nbservices/1

class API(unittest.TestCase):
    def setUp(self):
        self.mecid="1"
        self.service="1"
        self.server="http://192.168.10.9:8080/v0/"
        self.headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}
        self.platformuser = "5gmeta"
        self.platformpassword = "5Gm3t4!"
        self.auth_headers = api.get_header_with_token(platformuser, platformpassword)

    def test_delete_service(self):
        url = self.server+'mecregistry/'+self.mecid+"/nbservices/"+self.service
        x = requests.delete(url)

    def test_mec(self):
        url = server+'mecregistry/'+mecid+"/nbservices/"+serviceid
        r = requests.get(url)
        json_response=r.json()
        print(json_response)

    def test_case2(self):
        r = requests.get(url)
        json_response=r.json()
        print(json_response)

    def test_case3(self):
        x = requests.post(url, data=open('nb_register_service_example.json', 'rb'), headers=headers)
        json_response=x.json()
        print(json_response)

    def test_path(self):
        url = server+'mecregistry/'+mecid+"/nbservices/"+serviceid
        x = requests.patch(url, data=open('nb_update_service_example.json', 'rb'), headers=headers)
        json_response=x.json()
        print(json_response)

    def test_mec(self):
        mecid="2"
        tile="1222112111"
        server="http://5gmeta-platform.eu/discovery-api/"
        url = server+'mec/'+mecid+"/tile/"+tile
        x = requests.post(url,headers)
        json_response=x.json()
        print(json_response)
        #print("Given MEC id is: ", json_response['mec_id'])

    def test_mec(self):
        mecid="2"
        server="http://5gmeta-platform.eu/discovery-api/"
        url = server+'mec/'+mecid
        x = requests.delete(url,headers)
        print(x.text)

    def test_mec(self):
        mecid="2"
        tile="1222112111"
        server="http://5gmeta-platform.eu/discovery-api/"
        url = server+'mec/'+mecid+"/tile/"+tile
        x = requests.delete(url,headers)
        print(x.text)

    def test_mec(self):
        server = "http://5gmeta-platform.eu/discovery-api/"
        tile = "033111012102302012"
        url = server + 'mec'

        path = url + "/tile/" + tile

        r = requests.get(path, headers)
        json_response = r.json()

        print(json_response)
        # if (len(json_response) == 1):
        #    print("MEC ID where tile "+tile+" belong is :" + str(json_response[0]['id']))
        # else:
        #    print("Tile "+tile+" not found")

    def test_mec(self):
        server = "http://5gmeta-platform.eu/discovery-api/"
        mecid = "51"
        url = server + 'mec/'

        path = url + mecid

        r = requests.get(path, headers=headers)
        print(r.headers)
        json_response = r.json()

        print(json_response)
        # if (len(json_response) == 1):
        #    print("MEC ID where tile "+tile+" belong is :" + str(json_response[0]['id']))
        # else:
        #    print("Tile "+tile+" not found")
    def test_mec(self):
        server = "https://5gmeta-platform.eu/discovery-api/"
        url = server + 'mec'

        print(url)
        r = requests.get(url, headers=headers)
        print(r.headers)
        json_response = r.json()

        print(json_response)

    def test_mec(self):
        # server="http://5gmeta-platform.eu/discovery-api/"
        server = "http://192.168.10.9:8080/"
        url = server + 'mec'

        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        x = requests.post(url, data=open('sb_register_example.json', 'rb'), headers=headers)

        json_response = x.json()
        # print("Given MEC id is: ", json_response['mec_id'])
        print(json_response)