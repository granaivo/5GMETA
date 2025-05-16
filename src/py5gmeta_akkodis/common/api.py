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
