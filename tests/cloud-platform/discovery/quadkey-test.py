# pyGeoTile an example about how to use it
# ref. link: https://pygeotile.readthedocs.io/en/latest/
# @author damendola@vicomtech.org - Vicomtech
#
# 5GMETA project
#
# Required install: pip install pyGeoTile

from pygeotile.tile import Tile

lat = 43.292773 # Lat and lng of Vicomtech headquarter
lng = -1.986466
zoom = 18
tile = Tile.for_latitude_longitude(lat, lng, zoom)  # Tile Map Service (TMS) X Y and zoom

print('QuadTree (',lat , ', ', lng, ', ', zoom ,') : ', tile.quad_tree)  # QuadTree:  0302222310303211330
