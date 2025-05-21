#!/usr/bin/python3
# coding=utf-8

from py5gmeta.common.platform import FiveGMetaPlatform
platform_url = "https://cloudplatform.francecentral.cloudapp.azure.com"
bootstrap_port = "31081"
registry_port = "443"
username =  ""
password = ""
amqp_user = "5gmeta-platform"
amqp_password = "5gmeta-platform"
realm_name =  "5gmeta"
client_id = "5gmeta_login"
client_secret = ""


if __name__ == "__main__":
    platform = FiveGMetaPlatform(platform_url, amqp_user, amqp_password, bootstrap_port, registry_port, realm_name, client_id, client_secret, username, password)
    platform.run()
