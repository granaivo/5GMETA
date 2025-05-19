import requests
import sys
from py5gmeta.common import identity


def get_types(url:str, auth_header, mec_id:int):
    """ Get instance Types available on the MEC server"""
    try:
        types = requests.get(url + "/mecs/" + str(mec_id) + "/types", headers=auth_header).text
        return types
    except requests.exceptions.RequestException as err:
#        print(f"{err}")
        sys.exit("Error getting the instance types. Try again.")
    
def request_instance(url:str, auth_header, mec_id:int, data):
    """ Get instance Types available on the MEC server"""
    try:
        #auth_header['Content-Type'] = 'application/json'
        r = requests.post(url + "/mecs/" + str(mec_id) + "/instances", headers=auth_header, data=data)
        r.raise_for_status()
        json_response = r.json()
        
        return json_response
    except requests.exceptions.HTTPError as err:
#        print(f"{err}")
        sys.exit("Error deploying the instance of the selected pipeline. Try again.")


def delete_instance(url, auth_header, mec_id, instance_id):
    """ Delete an instance Type from a MEC server"""
    requests.delete(url + "/mecs/" + mec_id + "/instances/" + instance_id, headers=auth_header)


def get_topic(url, platform_user, realm_name, client_id, client_secret, platform_password, tile, instance_type):
    """Get the Kafka topic for the given tile and instance type."""
    topic = ""
    if not "/identity/" in url:
        identity_url = url + "/identity/"
        headers = identity.get_header_with_token(identity_url, realm_name, client_id, client_secret, platform_user, platform_password)

        topic =  requests.post(url, headers=headers).text
    return topic

def get_datatype_from_tile(url, auth_header, tile):
    """ Get dataflow using a tile """
    try:

        datatypes = requests.get(url + "/datatypes/" + tile, headers=auth_header)
        if datatypes.status_code == 500:
            sys.exit("Error getting datatypes. Try again.")
        else:
            return datatypes.text
    except requests.exceptions.RequestException as err:
        print(f"{err}")
        sys.exit("Error getting datatypes. Try again.")


def request_recource(url, endpoint, auth_header, tile, data_type, sub_type,  instance_type, filters):
    query = f'/query?dataSubType={sub_type}&quadkey={tile}&instance_type={instance_type}'
    try:
            answer= requests.post(url + endpoint + data_type + query, headers=auth_header)
            if answer.status_code != 200:
                sys.exit("Error requesting topics. Try again.")
            else:
                topic=answer.text
                return topic
    except requests.exceptions.RequestException as err:
        print(f"{err}")
        sys.exit("Error requesting topics. Try again.")


def request_topic(url, auth_header, tile, data_type, sub_type,  instance_type, filters):
    return request_recource(url, "/topics/", auth_header, tile, data_type, sub_type,  instance_type, filters)


def get_ids(url, auth_header, tile, data_type, sub_type,  instance_type, filters=""):
    return request_recource(url, "/dataflows/", auth_header, tile, data_type, sub_type,  instance_type, filters)


def get_properties(url, auth_header, tile, data_type, sub_type,  instance_type, filters):
    return request_recource(url, "/datatypes/", auth_header, tile, data_type+"/properties/", sub_type, instance_type, filters)


def get_id_properties(url, auth_header, mec_id):
    try:
        r = requests.get(url + "/dataflows/" + mec_id , headers=auth_header)
        r.raise_for_status()
        properties = r.json()
        return properties
    except requests.exceptions.RequestException as err:
#        print(f"{err}")
        sys.exit("Error requesting id properties. Try again.")


# TODO refactor
def delete_topic(url, auth_header, topic):
    requests.delete(url + "/topics/" + topic, headers=auth_header)


def get_tiles(url, auth_header):
    try:
        tiles = requests.get(url + "/mec/tile", headers=auth_header).text
        return tiles

    except requests.exceptions.RequestException as err:
#        print(f"{err}")
        sys.exit("Error getting tiles. Try again.")
    

def get_mec_id(url, auth_header, tile):
    try:
        r = requests.get(url + "/mec/tile/" + tile, headers=auth_header)
        json_response = r.json()
            
        return json_response[0]['id']

    except requests.exceptions.RequestException as err:
#        print(f"{err}")
        sys.exit("Error getting MEC ID. Try again.")


def discover_sb_service(url, tile, service_name, auth_headers):
    service_ip=-1
    service_port=-1
    if auth_headers is None:
        print("Access Token invalid, aborting!!!")
    else:
        r = requests.get(url+"/mec/tile/"+str(tile), headers=auth_headers)
        json_response=r.json()
        if len(json_response) > 0: 
            for mec in json_response:
                for service in mec['sb_services']:
                    if service['service_name'] == service_name:
                        service_ip=service['ip']
                        service_port=service['port']
    return service_ip,service_port


# Send the JSON of the dataflow's metadata, and receive the dataflowId and the topic where to publish the messages
# TODO add x-user-info in the Request
# TODO Fix the bug in the API server related to this
def register(url, dataflow_metadata, tile, auth_headers, x_user_infor=""):
    service_name="registration-api"
    registration_host,  registration_port = discover_sb_service(url, tile , service_name, auth_headers)
    if registration_host == -1 or registration_port == -1:
        print(service_name+" service not found")
        return "",-1
        #exit(-1)

    r = requests.post(str(registration_host) + "/dataflows/", json = dataflow_metadata)
    r=r.json()
    dataflow_id = r['id']
    topic = r['topic']
    send = r['send']
    return dataflow_id, topic, send


# This method will update dataflow Info in order to tell infrastructure that given dataflow is still available
def keep_alive_dataflow(url, dataflow_metadata, dataflow_id, auth_headers):
    tile=dataflow_metadata["dataSourceInfo"]["sourceLocationInfo"]["locationQuadkey"]
    registration_host, registration_port = discover_sb_service(url, tile,"registration-api", auth_headers)
    r = requests.put(registration_host + "/dataflows/"+str(dataflow_id), json = (dataflow_metadata))
    return r
