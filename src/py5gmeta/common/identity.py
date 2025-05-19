from os import access

from keycloak import KeycloakOpenID

def configure_client(url, realm_name, client_id,  client_secret):

    return KeycloakOpenID(server_url=url, client_id=client_id, realm_name=realm_name, client_secret_key=client_secret)


def get_auth_token(url, realm_name, client_id, client_secret, username, password):
     client = configure_client(url, realm_name, client_id, client_secret)
     return client.token(username, password)


def get_header_with_token(url, realm_name, client_id, client_secret, username, password):
    access_token = "Bearer " + get_auth_token(url, realm_name, client_id, client_secret, username, password)['access_token']

    headers = {
    'Authorization': access_token,
    }

    return headers
