import requests
import sys

url = "https://your-mec-fqdn/dataflow-api"

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
            print(url + "/topics/" + datatype + "/query?instance_type=" + instance_type + "&quadkey=" + tile + filters)
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

def delete_topic(auth_header, topic):
    requests.delete(url + "/topics/" + topic, headers=auth_header)
