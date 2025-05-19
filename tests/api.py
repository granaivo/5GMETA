import unittest
import requests
from py5gmeta.common import api, identity

#curl -X DELETE -H "Content-Type: application/json" http://192.168.10.9:8080/v0/mecregistry/1/nbservices/1

class APITests(unittest.TestCase):
    def setUp(self):
        self.mecid="1"
        self.service="1"
        self.api_end_point ="https://cloudplatform.francecentral.cloudapp.azure.com/api/v1"
        self.identity_url = "https://cloudplatform.francecentral.cloudapp.azure.com/identity/"
        self.headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}
        self.username = "5gmeta"
        self.password = "5gmeta"
        self.realm_name = "5gmeta"
        self.client_id = "5gmeta_login"
        self.client_secret = ""
        self.auth_headers = identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id, self.client_secret,  self.username, self.password)
        self.service_id = "1"

    def test_delete_service(self):
        url = self.api_end_point+'/'+self.mecid+"/nbservices/"+self.service_id
        self.assertEqual(requests.delete(url, headers=self.auth_headers), "")

    def test_get_mec(self):
        url = self.api_end_point + '/' + self.mecid + "/nbservices/" + self.service
        self.assertEqual(requests.get(url, headers=self.auth_headers), "")

    def test_post_mec(self):
        url = self.api_end_point + '/' + self.mecid + "/nbservices/" + self.service
        self.assertEqual(requests.post(url, data=open('../datasets/nb_register_service_example.json', 'rb'), headers=self.auth_headers), "")

    def test_patch_mec(self):
        url = self.api_end_point + '/' + self.mecid + "/nbservices/" + self.service
        x = requests.patch(url, data=open('../datasets/nb_update_service_example.json', 'rb'), headers=self.auth_headers)
        self.assertEqual(x, "")


if __name__ == "__main__":
    unittest.main()