import os


from py5gmeta.common import api

def get_amqp_broker_url(host, port, username, password):
    return f'amqp://{username}:{password}@{host}:{port}'

def get_message_video_broker_url():
    return get_url() + '/topic://video'

def get_url():
    username =  os.getenv('AMQP_USER')
    password = os.getenv('AMQP_PASS')
    host = os.getenv("AMQP_HOST")
    port = os.getenv('AMQP_PORT')

    return get_amqp_broker_url(host, port, username, password)


def get_message_broker_address_by_tile(url, tile, auth_headers):
    # Get Message Broker access
    service_name="message-broker"
    amqp_broker_host, amqp_broker_port = api.discover_sb_service(url, tile, service_name, auth_headers)
    if amqp_broker_host == "" or amqp_broker_port == 0:
        raise Exception("Error while getting the borker host and port number")

    username =  os.getenv('AMQP_USER')
    password =  os.getenv('AMQP_PASSWORD')
    return get_amqp_broker_url(str(amqp_broker_host), str(amqp_broker_port), username,  password )