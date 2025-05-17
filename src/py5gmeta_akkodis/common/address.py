import os
from py5gmeta.common import api


def get_url(host, port, username, password, ):
    return 'amqp://' + username + ":" + password + '@' + host + ":" + port


def get_message_video_borker_url():
    return get_url() + '/topic://video'


def get_url():
    username =  os.getenv('AMQP_USER')
    password = .getenv('AMQP_PASS')
    host = os.getenv("AMQP_HOST")
    port = os.getenv('AMQP_PORT')

    return get_url(host, port, username, password)


def get_message_data_address(tile, topic):
    # Get Message Broker access
    service="message-broker"
    messageBroker_host, messageBroker_port = api.discover_sb_service(tile,service)
    if messageBroker_host == -1 or messageBroker_port == -1:
        print(service+" service not found")
        return -1
     username =  os.getenv('AMQP_USER')
     password =  os.getenv('AMQP_PASSWORD')

     return get_url(str(messageBroker_host), str(messageBroker_port), username,  password ) +":/topic://"+topic


