import json
import random
from requests import get
from pygeotile.tile import Tile
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="py5gmeta")

def get_tile_by_position(latitude, longitude, zoom=18):
    tile = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=zoom)
    return  str(tile.quad_tree)

# Pseudo movement to get different MEC coverage
def gen_moving_gps(latitude,longitude):

    lat_rand=random.uniform(0.001,-0.02)
    long_rand=random.uniform(0.01,-0.01)

    lat= latitude+lat_rand
    long =longitude+long_rand
    return lat, long

def get_tile_by_url(url='https://api.ipify.org', zoom=16):
    ip = get(url).text
    ip_info = json.loads(get('https://ipinfo.io/' + ip).text)
    location = [float(i) for i in ip_info.get('loc').split(',')]
    quadkey = Tile.for_latitude_longitude(location[0],location[1],zoom).quad_tree
    print(quadkey)

    return quadkey

def get_location(latitude, longitude):
    return geolocator.reverse([latitude, longitude], language="en")

def get_address(latitude, longitude):
    return get_location(latitude, longitude).raw['address'].get('country', '')

def get_country_from(latitude, longitude):
    return get_address(latitude, longitude)
