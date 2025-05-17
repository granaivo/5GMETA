import sys
import json
from requests import get
from pygeotile.tile import Tile


def get_tile_by_position(latitude, longitude,zoom=18):
    tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=zoom)
    tile=str(tileTmp.quad_tree)
    return tile


def get_tile_by_url(url='https://api.ipify.org', zoom=16):

    ip = get(url).text
    ip_info = json.loads(get('https://ipinfo.io/' + ip).text)
    location = [float(i) for i in ip_info.get('loc').split(',')]
    quadkey = Tile.for_latitude_longitude(location[0],location[1],zoom).quad_tree

    print(quadkey)

    return quadkey

