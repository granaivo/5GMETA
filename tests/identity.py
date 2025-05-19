from  py5gmeta.common import identity
import unittest
import requests


class Identity(unittest.TestCase):
    def setUp(self):
        self.genuine_username = ""
        self.genuine_user_password = ""
        self.rogue_user_name = ""
        self.roque_user_password = ""

    def test_genuine_user_authentication(self):
         auth_header = identity.get_header_with_token(self.genuine_username, self.genuine_user_password)
         print(auth_header)

    def test_rogue_user_authentication(self):
        pass


    def test_get_auth_token(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        data = {
            'grant_type': 'password',
            'username': self.genuine_username,
            'password': self.genuine_user_password,
            'client_id': '5gmeta_login',
        }
        response = requests.post('https://5gmeta-platform.eu/identity/realms/5gmeta/protocol/openid-connect/token',
                                 headers=headers, data=data)
        r = response.json()

        return r['access_token']

    def test_get_header_with_token(self):
        token = "Bearer " + self.test_get_auth_token()
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        return headers


