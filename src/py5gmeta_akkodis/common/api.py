import requests
import sys

url = "https://cloudplatform.francecentral.cloudapp.azure.com/api/v1"

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



def get_datatype_from_tile(auth_header, tile):
    try:
        datatypes = requests.get(url + "/datatypes/" + tile, headers=auth_header)
        if datatypes.status_code == 500:
            sys.exit("Error getting datatypes. Try again.")
        else:
            return datatypes.text
    except Exception as err:
#        print(f"{err}")
        sys.exit("Error getting datatypes. Try again.")
    
def request_topic(auth_header, tile, datatype, instance_type="", filters=""):
    try:
        if instance_type == "":
            answer= requests.post(url + "/topics/" + datatype + "/query?quadkey=" + tile + filters, headers=auth_header)
            if answer.status_code != 200:
                sys.exit("Error requesting topics. Try again.")
            else:
                topic=answer.text
                return topic
        else:
            answer = requests.post(url + "/topics/" + datatype + "/query?instance_type=" + instance_type + "&quadkey=" + tile + filters, headers=auth_header)
            if answer.status_code != 200:
                sys.exit("Error requesting topics. Try again.")
            else:
                topic=answer.text
                return topic
    except Exception as err:
        print(f"{err}")
        sys.exit("Error requesting topics. Try again.")

def get_ids(auth_header, tile, datatype, filters=""):
    try:
        r = requests.get(url + "/dataflows/" + datatype + "/query?quadkey=" + tile + filters, headers=auth_header)
        r.raise_for_status()
        ids = r.json()
        
        return ids
    except Exception as err:
#        print(f"{err}")
        sys.exit("Error requesting source ids. Try again.")

def get_properties(auth_header, datatype):
    try:
        r = requests.get(url + "/datatypes/" + datatype + "/properties", headers=auth_header)
        r.raise_for_status()
        properties = r.json()
        
        return properties
    except Exception as err:
#        print(f"{err}")
        sys.exit("Error requesting datatype properties. Try again.")

def get_id_properties(auth_header, id):
    try:
        r = requests.get(url + "/dataflows/" + id , headers=auth_header)
        r.raise_for_status()
        properties = r.json()
        
        return properties
    except Exception as err:
#        print(f"{err}")
        sys.exit("Error requesting id properties. Try again.")


def delete_topic(auth_header, topic):
    requests.delete(url + "/topics/" + topic, headers=auth_header)


def get_tiles(auth_header):
    try:
        tiles = requests.get(url + "/mec/tile", headers=auth_header).text

        return tiles

    except Exception as err:
#        print(f"{err}")
        sys.exit("Error getting tiles. Try again.")
    
def get_mec_id(auth_header, tile):
    try:
        r = requests.get(url + "/mec/tile/" + tile, headers=auth_header)
        json_response = r.json()
            
        return json_response[0]['id']

    except Exception as err:
#        print(f"{err}")
        sys.exit("Error getting MEC ID. Try again.")

import requests
import security


def discover_sb_service(tile,serviceName):
    headers = security.get_header_with_token()
    service_ip=-1
    service_port=-1
    if headers is None:
        print("Access Token invalid, aborting!!!")
    else:
        discovery_url="https://your-mec-fqdn//discovery-api"
        path=discovery_url+"/mec/tile/"+str(tile)
        r = requests.get(path, headers=headers)
        json_response=r.json()
        if len(json_response) > 0: 
            for mec in json_response:
                for service in mec['sb_services']:
                    if service['service_name'] == serviceName:
                        service_ip=service['ip']
                        service_port=service['port']
    return service_ip,service_port




def register(dataflowmetadata,tile):
    service="registration-api"
    registrationAPI_ip,  registrationAPI_port = discover_sb_service(tile,service)
    if registrationAPI_ip == -1 or registrationAPI_port == -1:
        print(service+" service not found")
        return -1,-1
        #exit(-1)


    url = "https://your-mec-fqdn/registration-api/dataflows"

    # Send the JSON of the dataflow's metadata, and receive the dataflowId and the topic where to publish the messages
    r = requests.post(url, json = dataflowmetadata)
    r=r.json()
    dataflowId = r['id']
    topic = r['topic']
    send = r['send']
    return dataflowId,topic,send


# This method will update dataflow Info in order to tell infrastructure that given dataflow is still available
def keepAliveDataflow(dataflowMetadata,dataflowId):
    tile=dataflowMetadata["dataSourceInfo"]["sourceLocationInfo"]["locationQuadkey"]
    registration_ip, registration_port=discover_sb_service(tile,"registration-api")

    url = "https://your-mec-fqdn/registration-api"
    r = requests.put(url+"/dataflows/"+str(dataflowId), json = (dataflowMetadata))
    return r


