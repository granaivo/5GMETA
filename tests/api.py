import unittest
import requests
from py5gmeta.common import api, identity, message


#curl -X DELETE -H "Content-Type: application/json" http://192.168.10.9:8080/v0/mecregistry/1/nbservices/1

class APITestCase(unittest.TestCase):

    def setUp(self):
        self.mec_id= 9
        self.service="1"
        self.url = "https://cloudplatform.francecentral.cloudapp.azure.com/"
        self.api_end_point ="https://cloudplatform.francecentral.cloudapp.azure.com/api/v1"
        self.identity_url = "https://cloudplatform.francecentral.cloudapp.azure.com/identity/"
        self.headers = {'Accept' :'application/json', 'Content-Type' : 'application/json'}
        self.username = "test"
        self.password = "test"
        self.realm_name = "5gmeta"
        self.client_id = "5gmeta_login"
        self.client_secret = ""
        self.auth_headers = identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id, self.client_secret,  self.username, self.password)
        self.service_id = "1"
        self.data =  {
                       "description":"API to manage the SLAs and reservations in a 5GMETA MEC Server.",
                        "host":"cloudplatform.francecentral.cloudapp.azure.com",
                        "port":1000,
                         "props":"{}",
                         "service_name":"slaedge-api"
                      }
        self.x_user_info = identity.get_x_user_info(self.identity_url,  self.realm_name, self.client_id, self.client_secret, self.username, self.password )
        self.dataflow_id = ""

        self.service_name = "message-broker"
        self.topic = "test"
        self.filters = ""
        self.sub_type = "cam"
        self.data_type = "cits"
        self.instance_type = "noinstance"
        self.tile = 123456789
        self.instance_id = "1"
        self.dataflow_metadata = message.generate_metadata(self.data_type, self.sub_type, 1.0, "json",  "upload",
                                                           "France", "Europe",43.599998, 1.43333, 2, 3, 4, "api-unitest")

    def test_get_types(self):
        self.assertIsNotNone(api.get_types(self.api_end_point, self.auth_headers, self.mec_id))

    def test_request_instance(self):
        self.assertIsNotNone(api.request_instance(self.api_end_point, self.auth_headers, self.mec_id, self.data))

    def test_delete_instance(self):
        api.delete_instance(self.api_end_point, self.auth_headers, self.mec_id, self.instance_id)

    def test_get_topic(self):
        topic = api.get_topic(self.url, self.auth_headers)
        self.assertIsNotNone(topic)

    def test_get_data_from_tile(self):
        api.get_datatype_from_tile(self.api_end_point, self.auth_headers, self.tile)

    def test_request_resource(self):
        api.request_recource(self.url, "/mecs", self.auth_headers, self.tile, self.data_type, self.sub_type,  self.instance_type, self.filters)

    def test_request_topic(self):
        api.request_topic(self.url, self.auth_headers, self.tile, self.data_type, self.sub_type,  self.instance_type, self.filters)

    def test_get_ids(self):
        api.get_ids(self.url, self.auth_headers, self.tile, self.data_type, self.sub_type, self.instance_type, self.filters)

    def test_get_properties(self):
        api.get_properties(self.url, self.auth_headers, self.tile, self.data_type, self.sub_type,  self.instance_type, self.filters)

    def test_get_id_properties(self):
        api.get_id_properties(self.api_end_point, self.auth_headers, self.mec_id)

    def test_delete_topi(self):
        api.delete_topic(self.url, self.auth_headers, self.topic)

    def test_get_tiles(self):
        api.get_tiles(self.api_end_point, self.auth_headers)

    def test_get_mec_id(self):
        api.get_mec_id(self.api_end_point, self.auth_headers, self.tile)
    
    def test_discover_sb_service(self):
        api.discover_sb_service(self.api_end_point, self.tile, self.service_name, self.auth_headers)

    def test_register(self):
        api.register(self.api_end_point, self.dataflow_metadata, self.tile, self.auth_headers)

    def test_send_keep_alive(self):
        api.keep_alive_dataflow(self.api_end_point, self.dataflow_metadata, self.dataflow_id, self.auth_headers)

if __name__ == "__main__":
    unittest.main()