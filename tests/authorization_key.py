#!/usr/bin/python3
# coding=utf-8

import keycloak
import discovery
import dataflow
import cloudinstance
import re
import json
import sys
import optparse

from getpass import getpass

if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Client to connect to 5GMETA Cloud for requesting datatypes to consume in a certain region and instace type")

    parser.add_option("--disable-instanceapi", action="store_true", dest="disable_instanceapi", default=False,
                    help="disable-instanceapi checks (default %default)")
    # parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")

    opts, args = parser.parse_args()
    disable_instanceapi = opts.disable_instanceapi
    # disable_instanceapi = False

    broker_address = "your-mec-fqdn" 
    bootstrap_port = "31090"
    registry_port =  "31081"

    print(f"Welcome to 5GMETA Platform\n")
    print(f"Please sign in into the platform")

    username = input("Enter your username: ")
    password = getpass(prompt="Enter your password: ")
    # username = ""
    # password = ""
    auth_header = keycloak.get_header_with_token(username, password)
    print(auth_header)

