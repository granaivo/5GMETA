#!/usr/bin/python3
# coding=utf-8

#import os
from py5gmeta.common.platform import FIVEGMETAPLATFORM

#platform_address = os.environ['CLOUD_PLATFORM_HOST']
#bootstrap_port = os.environ['KAFKA_BOOTSTRAP_PORT']
#registry_port = os.environ['REGISTRY_PORT']
#username =  os.environ['CLOUD_PLATFORM_USERNAME']
#password = os.environ['CLOUD_PLATFORM_PASSWORD']
#amqp_user = os.environ['AMQP_USER']
#amqp_password = os.environ['AMQP_PASSWORD']
#client_id = os.environ['CLIENT_ID']
#client_secret = os.environ['CLIENT_SECRET']


platform_url = "https://cloudplatform.francecentral.cloudapp.azure.com"
bootstrap_port = "31081"
registry_port = "443"
username =  "test"
password = "test"
amqp_user = "5gmeta-platform"
amqp_password = "5gmeta-platform"
realm_name =  "5gmeta"
client_id = "5gmeta_login"
client_secret = ""

if __name__ == "__main__":

    platform = FIVEGMETAPLATFORM(platform_url, amqp_user, amqp_password, bootstrap_port, registry_port, realm_name, client_id, client_secret, username, password)
    platform.run()
