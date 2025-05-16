import requests

def get_auth_token():
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
    'grant_type': 'password',
    'username': 'test',
    'password': 'test',
    'client_id': '5gmeta_login',
    }
    response = requests.post('https://your-mec-fqdn/identity/realms/5gmeta/protocol/openid-connect/token', headers=headers, data=data)
    r = response.json()
    return r['access_token']

def get_header_with_token():
    token = "Bearer " + get_auth_token()

    headers = {
    'Authorization': token,
    }

    return headers
