from py5gmeta.common import geotile

def generate_metadata(datatype, subtype, dataformat, direction, country, latitude, longitude):
    # Replace with your metadata
    tile = geotile.get_tile_by_position(latitude, longitude)
    dataflowmetadata = {
        "dataTypeInfo": {
            "dataType": datatype,
            "dataSubType": subtype
        },
        "dataInfo": {
            "dataFormat": dataformat,
            "dataSampleRate": 0.0,
            "dataflowDirection": direction,
            "extraAttributes": None,
        },
        "licenseInfo": {
            "licenseGeolimit": "europe",
            "licenseType": "profit"
        },
        "dataSourceInfo": {
            "sourceTimezone": 2,
            "sourceStratumLevel": 3,
            "sourceId": 1,
            "sourceType": "vehicle",
            "sourceLocationInfo": {
                "locationQuadkey": tile,
                "locationCountry": country,
                "locationLatitude": latitude,
                "locationLongitude": longitude
            }
        }
    }
    return dataflowmetadata


class EventMessage(object):
    """
    Message record
    Args:
        message_timestamp
        correlation_id
        redelivered
        reply_to
        destination
        message_id
        mode
        type
        priority
        payload
        properties: {}
    """
    def __init__(self,  correlation_id='',
                    redelivered=False, reply_to='', destination='', message_id='',
                    mode=4, otype='', priority=1, payload='', properties=None): #{'source_id': 's0'}
        self.correlation_id = correlation_id
        self.redelivered = redelivered
        self.reply_to = reply_to
        self.destination = destination
        self.message_id = message_id
        self.mode = mode
        self.type   = otype
        self.priority = priority
        self.payload = payload
        self.properties = properties
        print(self.payload)
        print(self.properties)


