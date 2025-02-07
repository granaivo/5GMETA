import requests
import sys

url = "https://your-mec-fqdn/cloudinstance-api"

def get_types(auth_header, mec_id):
    try:
        types = requests.get(url + "/mecs/" + mec_id + "/types", headers=auth_header).text
        return types
    except Exception as err:
#        print(f"{err}")
        sys.exit("Error getting the instance types. Try again.")
    
def request_instance(auth_header, mec_id, data):
    try:
        auth_header['Content-Type'] = 'application/json'
        r = requests.post(url + "/mecs/" + mec_id + "/instances", headers=auth_header, data=data)
        r.raise_for_status()
        json_response = r.json()
        
        return json_response
    except requests.exceptions.HTTPError as err:
#        print(f"{err}")
        sys.exit("Error deploying the instance of the selected pipeline. Try again.")

def delete_instance(auth_header, mec_id, instance_id):
    requests.delete(url + "/mecs/" + mec_id + "/instances/" + instance_id, headers=auth_header)

