import requests
import sys

url = "https://your-mec-fqdn/discovery-api"

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
