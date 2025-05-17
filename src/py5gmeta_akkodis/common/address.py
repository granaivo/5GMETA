import os


def get_tile(latitude, longitude,zoom=18):
    tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=zoom)
    tile=str(tileTmp.quad_tree)
    return tile


def get_video_borker_url():
    url = 'amqp://' + os.getenv('AMQP_USER') + ":" + os.getenv('AMQP_PASS') + '@' + os.getenv("AMQP_IP") + ":" + os.getenv('AMQP_PORT') + '/topic://video'
    return url

def get_message_broker_address(tile):
    # Get Message Broker access
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        return -1

    address="amqp://"+username+":"+password+"@"+str(messageBroker_ip)+":"+str(messageBroker_port)+":/topic://"+topic
    return address
