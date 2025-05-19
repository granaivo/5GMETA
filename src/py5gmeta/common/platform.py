from getpass import getpass
from py5gmeta.common import api, helpers, identity
import sys
import re
from prometheus_client import start_http_server, Gauge

class FIVEGMETAPLATFORM:

    def __init__(self, platform_url, amqp_user, amqp_password, bootstrap_port, registry_port, realm_name, client_id, client_secret, username, password):
        self.platform_url = platform_url
        self.amqp_user = amqp_user
        self.amqp_password = amqp_password
        self.boostrap_port = bootstrap_port
        self.registry_port = registry_port
        self.realm_name = realm_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.disable_instance_api = True
        self.gauge = Gauge( "application_latency","Application Latency.")


    def get_platform_url(self):
        return  self.platform_url

    def get_realm_name (self):
       return self.realm_name

    def get_client_id(self):
       return self.client_id

    def get_client_name(self):
       return self.get_client_name()

    def get_client_secret(self):
        return self.client_secret

    def get_bootstrap_port(self):
        return self.boostrap_port
    def get_username(self):
        return self.username

    def get_password(self):
        return self.password

    def get_registry_port(self):
        return self.registry_port

    def get_platform_api_endpoint(self):
        return self.get_platform_url() + "/api/v1"

    def get_platform_identity_url(self):
        return self.get_platform_url() + "/identity/"

    def get_platform_topics_url(self):
        return  self.get_platform_api_endpoint() +  "/topics/cits/query?dataSubType=json&quadkey={tile}&instance_type={instance_type}"

    def set_username(self):
        if not self.get_password() :
         self.username = input("Enter your username: ")

    def set_password(self):
        if not self.get_password():
            self.password = getpass(prompt="Enter your password: ")

    def print_welcome(self):

        print(f"Welcome to 5GMETA Platform\n")

        if not self.get_username():
            print(f"Please sign in into the platform")
            self.set_username()
            self.set_password()

    def print_topics_information(self):
        auth_header = identity.get_header_with_token(self.get_platform_identity_url(), self.get_realm_name(),
                                                     self.get_client_id(),
                                                     self.get_client_secret(), self.get_username(), self.get_password())
        choice = helpers.produce_or_consume()
        helpers.print_topics_information(url=self.get_platform_api_endpoint(),auth_header=auth_header, choice=choice, api_endpoint= self.get_platform_api_endpoint(), disable_instance_api= self.disable_instance_api, username = self.get_username(), password= self.get_platform_topics_url(), identity_url= self.get_platform_identity_url(), realm_name =  self.get_realm_name(), client_id= self.get_client_id(), client_secret= self.get_client_secret())

    def run(self):
        self.print_welcome()
        self.print_topics_information()
        print("Thank your for using the 5GMETA Platform. Bye.")


