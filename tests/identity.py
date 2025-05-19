from keycloak import KeycloakAuthenticationError

from  py5gmeta.common import identity
import unittest

class Identity(unittest.TestCase):
    def setUp(self):
        self.genuine_username = "5gmeta-platform"
        self.genuine_user_password = "5gmeta-platform"
        self.rogue_user_name = "Alice"
        self.roque_user_password = "Alice-4gmeta"
        self.realm_name = "5gmeta"
        self.client_id = "5gmeta_login"
        self.client_secret = ""
        self.identity_url = "https://cloudplatform.francecentral.cloudapp.azure.com/identity/"

    def test_genuine_user_authentication(self):
        auth_headers = identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id,
                                                           self.client_secret, self.genuine_username, self.genuine_user_password)
        print(auth_headers)

    def test_rogue_user_authentication(self):
        self.assertRaises(KeycloakAuthenticationError, identity.get_header_with_token(self.identity_url, self.realm_name, self.client_id,
                                                           self.client_secret, self.rogue_user_name, self.roque_user_password) )


