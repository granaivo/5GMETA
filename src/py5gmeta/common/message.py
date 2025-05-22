from py5gmeta.common import geotile
import string
from proton import Message, symbol, ulong, PropertyDict
import base64
import sys
import random
import time

from py5gmeta.common.database import  DataInfo, DataSourceInfo, LicenseInfo, SourceLocationInfo, DataTypeInfo, DataflowMetaData


def generate_metadata(data_type: str, sub_type: str, data_sample_rate: float, dataformat: str, direction: str, country: str, geo_limit: str, latitude: float, longitude: float, source_time_zone: int, source_stratum_level: int, source_id: int, source_type: str):
    # Replace with your metadata
    tile = geotile.get_tile_by_position(latitude, longitude)
    country = geotile.get_country_from(latitude, longitude)
    data_type_info = DataTypeInfo(data_type, sub_type)
    data_info = DataInfo(dataformat, data_sample_rate, direction, None)
    license_info = LicenseInfo(country, geo_limit)
    source_location_info = SourceLocationInfo(tile, country, latitude, longitude)
    data_source_info = DataSourceInfo(source_time_zone, source_stratum_level, source_id, source_type, source_location_info)

    return  DataflowMetaData(data_type_info, data_info, license_info, data_source_info)




#messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(10)]


# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generate_random_group_id (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def messages_generator(msg_type: str, num: int, msg_body, flow_id, **kargs):
    messages = []
    if msg_type == "cits":
        messages =  cits_messages_generator(num, kargs['tile'], msg_body, flow_id)
    elif msg_type == "image":
        messages = image_messages_generator(kargs['image'], num, kargs['tile'], msg_body)
    return messages


def cits_messages_generator(num, tile, msg_body, data_flow_id, data_format='json'):
    messages = []
    #print("Sender prepare the messages... ")
    for i in range(num):
        props = {
                    "dataType": "cits",
                    "dataSubType": "cam",
                    "dataFlowId": data_flow_id,
                    "dataFormat": data_format,
                    "sourceId": 1,
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msg_body))
                    }
        message = Message(body=msg_body, properties=props)
        print(message)
        messages.append( message )
    return messages


def image_messages_generator(image, num, tile, msg_body ):
    messages = []
    if not msg_body :
        with open(image, "rb") as f:
            msg_body = base64.b64encode(f.read())
    print("Size of the image: " + str(sys.getsizeof(msg_body)))
    print("Sender prepare the messages... ")
    for i in range(num):
        props = {
                    "dataType": "image",
                    "dataSubType": "jpg",
                    "sourceId": "v"+str(i),
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msgbody))
                }
        messages.append( Message(body=msgbody, properties=props) )
        print(messages[i])
    print("Message array done! \n")
    return messages

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


