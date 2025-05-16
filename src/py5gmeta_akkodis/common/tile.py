import sys
import json
from requests import get
from pygeotile.tile import Tile


def get_tile(url='https://api.ipify.org', zoom=16):

    ip = get(url).text
    ip_info = json.loads(get('https://ipinfo.io/' + ip).text)
    location = [float(i) for i in ip_info.get('loc').split(',')]
    quadkey = Tile.for_latitude_longitude(location[0],location[1],zoom).quad_tree

    print(quadkey)

    return quadkey

