from keycloak import KeycloakAuthenticationError

from  py5gmeta.common import identity
import unittest

class Identity:
    def __init__(self):
        self.username = ""
        self.password = ""
        self.client_id = ""
        self.client_secrert = ""
        self.url = ""
        self.realm

class IdentityTestCase(unittest.TestCase):
    def setUp(self):
        self.genuine_username = "5gmeta-platform"
        self.genuine_user_password = "5gmeta-platform"
        self.rogue_user_name = "Alice"
        self.roque_user_password = "Alice-4gmeta"
        self.realm_name = "5gmeta"
        self.client_id = "5gmeta_login"
        self.client_secret = ""
        self.identity_url = "https://cloudplatform.francecentral.cloudapp.azure.com/identity/"

    def test_configure_client(self):
        pass

    def test_get_auth_token(self):
        pass

    def test_get_header_with_toke(self):
        pass

    def test_get_x_user_info(self):
        pass

    def test_genuine_user_authentication(self):
        auth_headers = identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id,
                                                           self.client_secret, self.genuine_username, self.genuine_user_password)
        print(auth_headers)

    def test_rogue_user_authentication(self):
        self.assertRaises(KeycloakAuthenticationError, identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id,
                                                           self.client_secret, self.rogue_user_name, self.roque_user_password) )


