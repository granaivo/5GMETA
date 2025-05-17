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

    tiles = []
    topics = []
    filters = ""
    source_ids = {}
    instance_ids = {}

    # ASK IF THE USER WANTS TO CONSUME OR PRODUCE DATA
    print(f"\nDo you want to consume data or produce an event?")
    print(f"For consuming data enter 'c' or 'C', for producing an event enter 'e' or 'E'")
    #while True:
    #    choice = input("Enter your choice: ")
    #    if choice == 'c' or choice == 'C' or choice == 'e' or choice == 'E':
    #        break
    choice = 'c'
    if choice == 'c' or choice == 'C':
        # REQUEST TILES WITH DATA AVAILABLE
        available_tiles = discovery.get_tiles(auth_header)
        available_tiles = list(filter(None,re.split('\[|\]|\"|,|\n|\s',available_tiles)))
        print(f"\nYou have data avalaible in the following tiles:")
        print(f"{available_tiles}")
        print(f"\nPlease enter the tiles where you want to consume data\nWhen done enter 'q' or 'Q'")
        #while True:
        tile = "1202200101311"
        exists = False
        for x in available_tiles:
            if x == tile or x.startswith(tile):
                exists = True
        if exists == True:
            tiles.append(tile)
        else:
            sys.exit(f"There is not data avalaible in tile {tile}")
    if choice == 'e' or choice == 'E':
        print(f"\nPlease enter the tiles where you want to produce events\nWhen done enter 'q' or 'Q'")
        while True:
            tile = input("Tile: ")
            if tile == 'q' or tile == 'Q':
                break
            else:
                tiles.append(tile)
            
    print(f"\nSelected tile(s): {tiles}\n")

    for tile in tiles:
        if choice == 'c' or choice == 'C':
            datatypes = []
            avalaible_datatypes = dataflow.get_datatype_from_tile(auth_header, tile)
            avalaible_datatypes = list(filter(None,re.split('\[|\]|\"|,|\n|\s',avalaible_datatypes)))
            print(f"You have the following datatypes in tile {tile}: ")
            print(f"{avalaible_datatypes}")
            if not avalaible_datatypes or not 'cits' in avalaible_datatypes:
                sys.exit(f"\nSorry, there are no datatypes avalaible in tile {tile}\n")
            else: 
                #print(f"\nPlease enter the datatype you want to consume in tile {tile}\nWhen done enter 'q' or 'Q'")
                #while True:
                datatype = 'cits'
                #    if datatype == 'q' or datatype == 'Q':
                #        break
                #    else:
                #        if datatype in avalaible_datatypes:
                datatypes.append(datatype)
                #        else:
                #            print(f"There is no {datatype} datatype avalaible in tile {tile}")
                        
                print(f"\nSelected datatype(s): {datatypes}\n")

                mec_id = str(discovery.get_mec_id(auth_header, tile))

                # REQUEST TOPICS TO DATAFLOW API           
                for datatype in datatypes:
                    # REQUEST INSTANCES TO INSTANCE API
                    if not disable_instanceapi:
                        instance_ids[mec_id] = []
                        avalaible_types_wide = cloudinstance.get_types(auth_header, mec_id)
                        avalaible_types_json = json.loads(avalaible_types_wide)
                        avalaible_types = [d['type_name'] for d in avalaible_types_json]
                        print(f"You have the following instance types in tile {tile}: ")
                        print(f"{avalaible_types_wide}")

                        instance_type = ""
                        #while instance_type not in avalaible_types:
                        #instance_type = input(f"Please enter the instance type for '{datatype}' pipeline in tile {tile}: ")
                        instance_type = "small"
                        if instance_type not in avalaible_types:
                            print(f"There is no '{instance_type}' instance type in tile {tile}")
                        data = '{"username": "' + username + '", "datatype": "' + datatype + '", "instance_type": "' + instance_type + '"}'
                        instance = cloudinstance.request_instance(auth_header, mec_id, data)
                        try:
                            instance_id = instance['instance_id']
                            instance_ids[mec_id].append(instance_id)
                            print(f"\nSelected instance type for '{datatype}' pipeline: {instance_type}\n")
                        except:
                            print(instance)
                            sys.exit("Please try again with an instancetype with lower requirements")
                    else:
                        instance_type = "small"

                    #print(f"You have the following subdatatypes in tile {tile}: ")
                    #datatype_properties = dataflow.get_properties(auth_header, datatype)
                    #avalaible_subdatatypes = datatype_properties['dataSubType']
                    #print(f"{avalaible_subdatatypes}")
                    #print(f"\nPlease enter the subdatatype of {datatype} datatype you want to consume: ")
                    #subdatatype = input("Subdatatype: ")
                    #filters = subdatatype
                    #filters = '&dataSubType=json'
                    topic = dataflow.request_topic(auth_header, tile, datatype, instance_type, filters)
                    topics.append(topic)

                    if datatype == "video":
                        ids = dataflow.get_ids(auth_header, tile, datatype, filters)
                        source_ids[tile] = ids
        if choice == 'e' or choice == 'E':
            instance_type = "noinstance"
            datatype = "event"
            topic = dataflow.request_topic(auth_header, tile, datatype, instance_type)
            topics.append(topic)
        
    if topics:
        print(f"Connect to the following kafka topics: {topics}")
        print(f"Kafka broker address: {broker_address}")
        print(f"Kafka bootstrap port: {bootstrap_port}")
        print(f"Kafka schema registry port: {registry_port}")
        if source_ids:
            print(f"Source IDs for selecting the individual video flows: {source_ids}")
        
        print(f"Execute the following command: \n\t  python example2_mapdisplay.py {topics[0]} {broker_address} {bootstrap_port} {registry_port}")
        while True:
            quit = input("\nTo stop using the platform please enter 'q' or 'Q': ")
            if quit == 'q' or quit == 'Q':
                print(f"\nExiting...")
                auth_header = keycloak.get_header_with_token(username, password)
                if choice == 'c' or choice == 'C':
                    if not disable_instanceapi:
                        for mec_id in instance_ids.keys():
                            for instance_id in instance_ids[mec_id]:
                                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
                for topic in topics:
                    dataflow.delete_topic(auth_header, topic)
                sys.exit("\nThank you for using 5GMETA Platform. Bye!")
            else:
                pass
    else:
        sys.exit("You did not request any data. Thank you for using 5GMETA Platform. Bye!")

    #while True:
    #    try:
    #        pass
    #    except KeyboardInterrupt:
    #        print(f"\nExiting...")
    #        for mec_id in instance_ids.keys():
    #            for instance_id in instance_ids[mec_id]:
    #                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
    #        for topic in topics:
    #            dataflow.delete_topic(auth_header, topic)
    #        sys.exit("\nThank you for using 5GMETA Platform. Bye!")
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

    tiles = []
    topics = []
    filters = ""
    source_ids = {}
    instance_ids = {}

    # ASK IF THE USER WANTS TO CONSUME OR PRODUCE DATA
    print(f"\nDo you want to consume data or produce an event?")
    print(f"For consuming data enter 'c' or 'C', for producing an event enter 'e' or 'E'")
    #while True:
    #    choice = input("Enter your choice: ")
    #    if choice == 'c' or choice == 'C' or choice == 'e' or choice == 'E':
    #        break
    choice = 'c'
    if choice == 'c' or choice == 'C':
        # REQUEST TILES WITH DATA AVAILABLE
        available_tiles = discovery.get_tiles(auth_header)
        available_tiles = list(filter(None,re.split('\[|\]|\"|,|\n|\s',available_tiles)))
        print(f"\nYou have data avalaible in the following tiles:")
        print(f"{available_tiles}")
        print(f"\nPlease enter the tiles where you want to consume data\nWhen done enter 'q' or 'Q'")
        #while True:
        tile = "1202200101311"
        exists = False
        for x in available_tiles:
            if x == tile or x.startswith(tile):
                exists = True
        if exists == True:
            tiles.append(tile)
        else:
            sys.exit(f"There is not data avalaible in tile {tile}")
    if choice == 'e' or choice == 'E':
        print(f"\nPlease enter the tiles where you want to produce events\nWhen done enter 'q' or 'Q'")
        while True:
            tile = input("Tile: ")
            if tile == 'q' or tile == 'Q':
                break
            else:
                tiles.append(tile)
            
    print(f"\nSelected tile(s): {tiles}\n")

    for tile in tiles:
        if choice == 'c' or choice == 'C':
            datatypes = []
            avalaible_datatypes = dataflow.get_datatype_from_tile(auth_header, tile)
            avalaible_datatypes = list(filter(None,re.split('\[|\]|\"|,|\n|\s',avalaible_datatypes)))
            print(f"You have the following datatypes in tile {tile}: ")
            print(f"{avalaible_datatypes}")
            if not avalaible_datatypes or not 'cits' in avalaible_datatypes:
                sys.exit(f"\nSorry, there are no datatypes avalaible in tile {tile}\n")
            else: 
                #print(f"\nPlease enter the datatype you want to consume in tile {tile}\nWhen done enter 'q' or 'Q'")
                #while True:
                datatype = 'cits'
                #    if datatype == 'q' or datatype == 'Q':
                #        break
                #    else:
                #        if datatype in avalaible_datatypes:
                datatypes.append(datatype)
                #        else:
                #            print(f"There is no {datatype} datatype avalaible in tile {tile}")
                        
                print(f"\nSelected datatype(s): {datatypes}\n")

                mec_id = str(discovery.get_mec_id(auth_header, tile))

                # REQUEST TOPICS TO DATAFLOW API           
                for datatype in datatypes:
                    # REQUEST INSTANCES TO INSTANCE API
                    if not disable_instanceapi:
                        instance_ids[mec_id] = []
                        avalaible_types_wide = cloudinstance.get_types(auth_header, mec_id)
                        avalaible_types_json = json.loads(avalaible_types_wide)
                        avalaible_types = [d['type_name'] for d in avalaible_types_json]
                        print(f"You have the following instance types in tile {tile}: ")
                        print(f"{avalaible_types_wide}")

                        instance_type = ""
                        #while instance_type not in avalaible_types:
                        #instance_type = input(f"Please enter the instance type for '{datatype}' pipeline in tile {tile}: ")
                        instance_type = "small"
                        if instance_type not in avalaible_types:
                            print(f"There is no '{instance_type}' instance type in tile {tile}")
                        data = '{"username": "' + username + '", "datatype": "' + datatype + '", "instance_type": "' + instance_type + '"}'
                        instance = cloudinstance.request_instance(auth_header, mec_id, data)
                        try:
                            instance_id = instance['instance_id']
                            instance_ids[mec_id].append(instance_id)
                            print(f"\nSelected instance type for '{datatype}' pipeline: {instance_type}\n")
                        except:
                            print(instance)
                            sys.exit("Please try again with an instancetype with lower requirements")
                    else:
                        instance_type = "small"

                    #print(f"You have the following subdatatypes in tile {tile}: ")
                    #datatype_properties = dataflow.get_properties(auth_header, datatype)
                    #avalaible_subdatatypes = datatype_properties['dataSubType']
                    #print(f"{avalaible_subdatatypes}")
                    #print(f"\nPlease enter the subdatatype of {datatype} datatype you want to consume: ")
                    #subdatatype = input("Subdatatype: ")
                    #filters = subdatatype
                    #filters = '&dataSubType=json'
                    topic = dataflow.request_topic(auth_header, tile, datatype, instance_type, filters)
                    topics.append(topic)

                    if datatype == "video":
                        ids = dataflow.get_ids(auth_header, tile, datatype, filters)
                        source_ids[tile] = ids
        choice2='e'
        if choice2 == 'e' or choice2 == 'E':
            instance_type = "noinstance"
            datatype = "event"
            topic = dataflow.request_topic(auth_header, tile, datatype, instance_type)
            topics.append(topic)
        
    if topics:
        print(f"Connect to the following kafka topics: {topics}")
        print(f"Kafka broker address: {broker_address}")
        print(f"Kafka bootstrap port: {bootstrap_port}")
        print(f"Kafka schema registry port: {registry_port}")
        if source_ids:
            print(f"Source IDs for selecting the individual video flows: {source_ids}")
        
        print(f"Execute the following command: \n\t  python example3_eventfeedback.py {topics[0]} {topics[1]} {broker_address} {bootstrap_port} {registry_port}")
        while True:
            quit = input("\nTo stop using the platform please enter 'q' or 'Q': ")
            if quit == 'q' or quit == 'Q':
                print(f"\nExiting...")
                auth_header = keycloak.get_header_with_token(username, password)
                if choice == 'c' or choice == 'C':
                    if not disable_instanceapi:
                        for mec_id in instance_ids.keys():
                            for instance_id in instance_ids[mec_id]:
                                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
                for topic in topics:
                    dataflow.delete_topic(auth_header, topic)
                sys.exit("\nThank you for using 5GMETA Platform. Bye!")
            else:
                pass
    else:
        sys.exit("You did not request any data. Thank you for using 5GMETA Platform. Bye!")

    #while True:
    #    try:
    #        pass
    #    except KeyboardInterrupt:
    #        print(f"\nExiting...")
    #        for mec_id in instance_ids.keys():
    #            for instance_id in instance_ids[mec_id]:
    #                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
    #        for topic in topics:
    #            dataflow.delete_topic(auth_header, topic)
    #        sys.exit("\nThank you for using 5GMETA Platform. Bye!")
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

    tiles = []
    topics = []
    filters = ""
    source_ids = {}
    instance_ids = {}

    # ASK IF THE USER WANTS TO CONSUME OR PRODUCE DATA
    print(f"\nDo you want to consume data or produce an event?")
    print(f"For consuming data enter 'c' or 'C', for producing an event enter 'e' or 'E'")
    #while True:
    #    choice = input("Enter your choice: ")
    #    if choice == 'c' or choice == 'C' or choice == 'e' or choice == 'E':
    #        break
    choice = 'c'
    if choice == 'c' or choice == 'C':
        # REQUEST TILES WITH DATA AVAILABLE
        available_tiles = discovery.get_tiles(auth_header)
        available_tiles = list(filter(None,re.split('\[|\]|\"|,|\n|\s',available_tiles)))
        print(f"\nYou have data avalaible in the following tiles:")
        print(f"{available_tiles}")
        print(f"\nPlease enter the tiles where you want to consume data\nWhen done enter 'q' or 'Q'")
        #while True:
        tile = "1202200101311"
        exists = False
        for x in available_tiles:
            if x == tile or x.startswith(tile):
                exists = True
        if exists == True:
            tiles.append(tile)
        else:
            sys.exit(f"There is not data avalaible in tile {tile}")
    if choice == 'e' or choice == 'E':
        print(f"\nPlease enter the tiles where you want to produce events\nWhen done enter 'q' or 'Q'")
        while True:
            tile = input("Tile: ")
            if tile == 'q' or tile == 'Q':
                break
            else:
                tiles.append(tile)
            
    print(f"\nSelected tile(s): {tiles}\n")

    for tile in tiles:
        if choice == 'c' or choice == 'C':
            datatypes = []
            avalaible_datatypes = dataflow.get_datatype_from_tile(auth_header, tile)
            avalaible_datatypes = list(filter(None,re.split('\[|\]|\"|,|\n|\s',avalaible_datatypes)))
            print(f"You have the following datatypes in tile {tile}: ")
            print(f"{avalaible_datatypes}")
            if not avalaible_datatypes or not 'cits' in avalaible_datatypes:
                sys.exit(f"\nSorry, there are no datatypes avalaible in tile {tile}\n")
            else: 
                #print(f"\nPlease enter the datatype you want to consume in tile {tile}\nWhen done enter 'q' or 'Q'")
                #while True:
                datatype = 'cits'
                #    if datatype == 'q' or datatype == 'Q':
                #        break
                #    else:
                #        if datatype in avalaible_datatypes:
                datatypes.append(datatype)
                #        else:
                #            print(f"There is no {datatype} datatype avalaible in tile {tile}")
                        
                print(f"\nSelected datatype(s): {datatypes}\n")

                mec_id = str(discovery.get_mec_id(auth_header, tile))

                # REQUEST TOPICS TO DATAFLOW API           
                for datatype in datatypes:
                    # REQUEST INSTANCES TO INSTANCE API
                    if not disable_instanceapi:
                        instance_ids[mec_id] = []
                        avalaible_types_wide = cloudinstance.get_types(auth_header, mec_id)
                        avalaible_types_json = json.loads(avalaible_types_wide)
                        avalaible_types = [d['type_name'] for d in avalaible_types_json]
                        print(f"You have the following instance types in tile {tile}: ")
                        print(f"{avalaible_types_wide}")

                        instance_type = ""
                        #while instance_type not in avalaible_types:
                        #instance_type = input(f"Please enter the instance type for '{datatype}' pipeline in tile {tile}: ")
                        instance_type = "small"
                        if instance_type not in avalaible_types:
                            print(f"There is no '{instance_type}' instance type in tile {tile}")
                        data = '{"username": "' + username + '", "datatype": "' + datatype + '", "instance_type": "' + instance_type + '"}'
                        instance = cloudinstance.request_instance(auth_header, mec_id, data)
                        try:
                            instance_id = instance['instance_id']
                            instance_ids[mec_id].append(instance_id)
                            print(f"\nSelected instance type for '{datatype}' pipeline: {instance_type}\n")
                        except:
                            print(instance)
                            sys.exit("Please try again with an instancetype with lower requirements")
                    else:
                        instance_type = "small"

                    #print(f"You have the following subdatatypes in tile {tile}: ")
                    #datatype_properties = dataflow.get_properties(auth_header, datatype)
                    #avalaible_subdatatypes = datatype_properties['dataSubType']
                    #print(f"{avalaible_subdatatypes}")
                    #print(f"\nPlease enter the subdatatype of {datatype} datatype you want to consume: ")
                    #subdatatype = input("Subdatatype: ")
                    #filters = subdatatype
                    #filters = '&dataSubType=json'
                    topic = dataflow.request_topic(auth_header, tile, datatype, instance_type, filters)
                    topics.append(topic)

                    if datatype == "video":
                        ids = dataflow.get_ids(auth_header, tile, datatype, filters)
                        source_ids[tile] = ids
        choice2='e'
        if choice2 == 'e' or choice2 == 'E':
            instance_type = "noinstance"
            datatype = "event"
            topic = dataflow.request_topic(auth_header, tile, datatype, instance_type)
            topics.append(topic)
        
    if topics:
        print(f"Connect to the following kafka topics: {topics}")
        print(f"Kafka broker address: {broker_address}")
        print(f"Kafka bootstrap port: {bootstrap_port}")
        print(f"Kafka schema registry port: {registry_port}")
        if source_ids:
            print(f"Source IDs for selecting the individual video flows: {source_ids}")
        
        print(f"Execute the following command: \n\t  python example3_eventfeedback.py {topics[0]} {topics[1]} {broker_address} {bootstrap_port} {registry_port}")
        while True:
            quit = input("\nTo stop using the platform please enter 'q' or 'Q': ")
            if quit == 'q' or quit == 'Q':
                print(f"\nExiting...")
                auth_header = keycloak.get_header_with_token(username, password)
                if choice == 'c' or choice == 'C':
                    if not disable_instanceapi:
                        for mec_id in instance_ids.keys():
                            for instance_id in instance_ids[mec_id]:
                                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
                for topic in topics:
                    dataflow.delete_topic(auth_header, topic)
                sys.exit("\nThank you for using 5GMETA Platform. Bye!")
            else:
                pass
    else:
        sys.exit("You did not request any data. Thank you for using 5GMETA Platform. Bye!")

    #while True:
    #    try:
    #        pass
    #    except KeyboardInterrupt:
    #        print(f"\nExiting...")
    #        for mec_id in instance_ids.keys():
    #            for instance_id in instance_ids[mec_id]:
    #                cloudinstance.delete_instance(auth_header, mec_id, instance_id)
    #        for topic in topics:
    #            dataflow.delete_topic(auth_header, topic)
    #        sys.exit("\nThank you for using 5GMETA Platform. Bye!")
from time import sleep
from confluent_kafka import Consumer, KafkaException

import sys
import getopt
import json
import logging
from pprint import pformat

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition
import sys
import base64
import requests

#from proton.handlers import MessagingHandler
import proton
import random
import string

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from time import sleep
import threading

xmin = 0
xmax = 3
nbx = 151

x = np.linspace(xmin, xmax, nbx)
y = np.zeros(nbx)

polling_time = 1.0


def animate(i):
    y[1:] = y[:-1]
    y[0] = current_speed_value
    line.set_data(x, y)
    return line,

# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generateRandomGroupId (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def consume_data(topic,platformaddress, bootstrap_port,schema_registry_port):
    # Generate AvroConsumer schema
    c = AvroConsumer({
        'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
        'schema.registry.url':'http://'+platformaddress+':' + schema_registry_port, 
        'group.id': topic+'_'+generateRandomGroupId(4),
        'api.version.request': True,
        'auto.offset.reset': 'earliest'
    })

    # Subscribe to the topic from command line. MUST BE IN UPPERCASE
    c.subscribe([topic.upper()])

    print("Subscibed topics: " + str(topic))
    print("Running...")

    i = 0

    global current_speed_value
    global value_received

    # Start reading messages from Kafka topic
    while True:
        ## CONSUME DATA
        # Poll for messages
        msg = c.poll(polling_time)
        i+= 1

        if msg is None:
            print(".",  end="", flush=True)
            #last_received_value = (i%3) - 1
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Message received")
        # There is a valid Kafka message
        sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
            (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
        # The AVRO Message here in mydata
        print("Message received")
        mydata = msg.value() # .decode('latin-1') #.replace("'", '"')

        # The QPID proton message: this is the message sent from the S&D to the MEC
        raw_sd = mydata['BYTES_PAYLOAD']
        msg_sd = proton.Message()
        proton.Message.decode(msg_sd, raw_sd)

        # The msg_sd.body contains the data of the sendor
        data=msg_sd.body
        #print(data)
        #print(msg_sd.properties)
        props = str(msg_sd.properties)
        props = props.replace("\'","\"")
        #print(props)
        props = json.loads(props)
        if (not props['dataSubType']=='example1'):
            continue

        data_json = json.loads(data)
       
        value_received = True
        #(speed_val*1000*100/3600)
        current_speed_value = data_json["cam"]["camParameters"]["highFrequencyContainer"]["basicVehicleContainerHighFrequency"]["speed"]["speedValue"] * 3600 / (1000 * 100)
        #print(current_speed_value)


        '''print("Size " + str(sys.getsizeof(msg_sd.body)))

        outfile = open("../output/body_"+str(i)+".txt", 'w')
        i=i+1
        try:
            outfile.write(msg_sd.body)
        except:
            print("An error decoding the message happened!")
        
        outfile.close()
        '''
    c.close()

    
def display_help():
    print ("python example1_oscilloscope.py topic platformaddress bootstrap_port registry_port")

def main(argv):
    # my code here
    global current_speed_value
    global value_received
    value_received = False
    current_speed_value = 0
    if len(argv) != 4:
        print("missing or bad parameter")
        display_help();
        return

    fig = plt.figure() # initialise la figure
    global line
    line, = plt.plot([], []) 
    plt.xlim(xmin, xmax)
    plt.ylim(0, 70)

    # Get input parameters from command line
    topic=str(argv[0])
    platformaddress=str(argv[1])
    bootstrap_port=str(argv[2])
    schema_registry_port=str(argv[3])

    thread = threading.Thread(target = consume_data, args = (topic, platformaddress, bootstrap_port, schema_registry_port))
    thread.setDaemon(True)
    thread.start()

    ani = animation.FuncAnimation(fig, animate, interval=polling_time*1000, blit=True, repeat=False)
    plt.show()

    while True:
        sleep(polling_time)
        if value_received:
            print("New speed value from 5GMETA platfrom ", current_speed_value, "km/h")
            value_received = False

    thread.join()

    
if __name__ == "__main__":
    main(sys.argv[1:])


# Some sample code for the cosumere can be find 
# here https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_consumer.py

from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition
import sys
import base64
import requests

#from proton.handlers import MessagingHandler
import proton
import random
import string

import folium

latitude_ref = 48.786408
longitude_ref = 2.090822

m = folium.Map(location=[latitude_ref, longitude_ref], zoom_start=15)

# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generateRandomGroupId (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Ask the user to enter input parameters
if len(sys.argv) != 5:
    print("Usage: python3 cits-consumer.py topic platformaddress bootstrap_port registry_port ")
    exit()

# Get input parameters from command line
topic=str(sys.argv[1])
platformaddress=str(sys.argv[2])
bootstrap_port=str(sys.argv[3])
schema_registry_port=str(sys.argv[4])

# Generate AvroConsumer schema
c = AvroConsumer({
    'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
    'schema.registry.url':'http://'+platformaddress+':' + schema_registry_port, 
    'group.id': topic+'_'+generateRandomGroupId(4),
    'api.version.request': True,
    'auto.offset.reset': 'earliest'
})

# Subscribe to the topic from command line. MUST BE IN UPPERCASE
c.subscribe([topic.upper()])

print("Subscibed topics: " + str(topic))
print("Running...")

i = 0
max_count = 20

pts = []
lines = []
# Start reading messages from Kafka topic
while i<max_count:
    ## CONSUME DATA
    # Poll for messages
    msg = c.poll(1.0)

    if msg is None:
        print(".",  end="", flush=True)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    # There is a valid Kafka message
    sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
        (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
    # The AVRO Message here in mydata
    mydata = msg.value() # .decode('latin-1') #.replace("'", '"')

    # The QPID proton message: this is the message sent from the S&D to the MEC
    raw_sd = mydata['BYTES_PAYLOAD']
    msg_sd = proton.Message()
    proton.Message.decode(msg_sd, raw_sd)

    props = str(msg_sd.properties)
    props = props.replace("\'","\"")
    #print(props)
    props = json.loads(props)
    if (not props['dataSubType']=='example2'):
        continue

    # The msg_sd.body contains the data of the sendor
    data=msg_sd.body
    data_json = json.loads(data)

    ## PROCESS DATA TO SHOW ON MAP
    latitude = data_json["cam"]["camParameters"]["basicContainer"]["referencePosition"]["latitude"] / 10000000.0
    longitude = data_json["cam"]["camParameters"]["basicContainer"]["referencePosition"]["longitude"] / 10000000.0
    print(f"Received location information latitude={latitude}, longitude={longitude}")
    
    node = tuple((latitude,longitude))
    folium.Circle(node,radius=10,color="red").add_to(m)
    
    pts.append(node)
    
    i+=1
    print("Nb received datas: ",i,"/",max_count)
    '''print("Size " + str(sys.getsizeof(msg_sd.body)))

    outfile = open("../output/body_"+str(i)+".txt", 'w')
    i=i+1
    try:
        outfile.write(msg_sd.body)
    except:
        print("An error decoding the message happened!")
        
    outfile.close()
    '''

c.close()

folium.PolyLine(pts, color="green", weight=6, opacity=1).add_to(m)
m.save('5GMETADataMap.html')
from uuid import uuid4
from time import sleep
from confluent_kafka import Consumer, KafkaException

import sys
import getopt
import json
import logging
from pprint import pformat

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from event_message import EventMessage
from helpers import msg_to_dict, delivery_report

import sys
import base64
import requests

#from proton.handlers import MessagingHandler
import proton
import random
import string

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from time import sleep
import threading

polling_time = 1.0
current_data_nb=0
nbdata = 5
# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generateRandomGroupId (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def consume_data(topic,platformaddress, bootstrap_port,schema_registry_port):
    # Generate AvroConsumer schema
    c = AvroConsumer({
        'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
        'schema.registry.url':'http://'+platformaddress+':' + schema_registry_port, 
        'group.id': topic+'_'+generateRandomGroupId(4),
        'api.version.request': True,
        'auto.offset.reset': 'earliest'
    })

    # Subscribe to the topic from command line. MUST BE IN UPPERCASE
    c.subscribe([topic.upper()])

    print("Subscibed topics: " + str(topic))
    print("Running...")

    i = 0

    global current_data_nb

    # Start reading messages from Kafka topic
    while current_data_nb<nbdata:
        ## CONSUME DATA
        # Poll for messages
        msg = c.poll(polling_time)
        i+= 1

        if msg is None:
            print(".",  end="", flush=True)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # There is a valid Kafka message
        sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
            (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
        # The AVRO Message here in mydata
        mydata = msg.value() # .decode('latin-1') #.replace("'", '"')

        # The QPID proton message: this is the message sent from the S&D to the MEC
        raw_sd = mydata['BYTES_PAYLOAD']
        msg_sd = proton.Message()
        proton.Message.decode(msg_sd, raw_sd)

        # The msg_sd.body contains the data of the sendor
        data=msg_sd.body
        #print(data)
        #print(msg_sd.properties)
        props = str(msg_sd.properties)
        props = props.replace("\'","\"")
        #print(props)
        props = json.loads(props)
        if (not props['dataSubType']=='example1'):
            continue
        
        current_data_nb +=1
        print(f"Received {current_data_nb} message ({nbdata} expected)")


        '''print("Size " + str(sys.getsizeof(msg_sd.body)))

        outfile = open("../output/body_"+str(i)+".txt", 'w')
        i=i+1
        try:
            outfile.write(msg_sd.body)
        except:
            print("An error decoding the message happened!")
        
        outfile.close()
        '''
    print(f"No more data expected")
    c.close()

def produce_event(topic,platformaddress, bootstrap_port,schema_registry_port):
    message = str(sys.argv[1])

    schema_str = """
    {
        "connect.name": "com.datamountaineer.streamreactor.connect.jms",
        "fields": [
            {
                "default": null,
                "name": "message_timestamp",
                "type": [
                    "null",
                    "long"
                ]
            },
            {
                "default": null,
                "name": "correlation_id",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "redelivered",
                "type": [
                    "null",
                    "boolean"
                ]
            },
            {
                "default": null,
                "name": "reply_to",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "destination",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "message_id",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "mode",
                "type": [
                    "null",
                    "int"
                ]
            },
            {
                "default": null,
                "name": "type",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "priority",
                "type": [
                    "null",
                    "int"
                ]
            },
            {
                "default": null,
                "name": "payload",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "properties",
                "type": [
                    "null",
                    {
                        "type": "map",
                        "values": [
                            "null",
                            "string"
                        ]
                    }
                ]
            }
        ],
        "name": "jms",
        "namespace": "com.datamountaineer.streamreactor.connect",
        "type": "record"
    }
    """
    schema_registry_conf = {'url': 'http://'+platformaddress+':' + schema_registry_port}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer( schema_registry_client,
                                      schema_str,
                                      msg_to_dict )

    producer_conf = {'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing EventMessage records to topic {}. ^C to exit.".format(topic))
    producer.poll(0.0)
    
    while current_data_nb < nbdata:
        print("Waiting for event triggering")
        sleep(1.0)

    try:
        msg_props = {"message": 'Hello Feedback'}
        msg = EventMessage(properties=msg_props)

        producer.produce(topic=topic, key=str(uuid4()), value=msg, on_delivery=delivery_report)

    except ValueError:
        print("Invalid input, discarding record...")

    print("\\nFlushing records...")
    producer.flush()

    
def display_help():
    print ("python example3_eventfeedback.py consume_topic produce_topic platformaddress bootstrap_port registry_port")

def main(argv):
    # my code here
    global last_received_value
    last_received_value = 0
    if len(argv) != 5:
        print("missing or bad parameter")
        display_help();
        return

    # Get input parameters from command line
    consume_topic=str(argv[0])
    produce_topic=str(argv[1])
    platformaddress=str(argv[2])
    bootstrap_port=str(argv[3])
    schema_registry_port=str(argv[4])

    thread_consume = threading.Thread(target = consume_data, args = (consume_topic, platformaddress, bootstrap_port, schema_registry_port))
    thread_consume.setDaemon(True)
    thread_consume.start()

    thread_produce = threading.Thread(target = produce_event, args = (produce_topic, platformaddress, bootstrap_port, schema_registry_port))
    thread_produce.start()

    thread_consume.join()
    thread_produce.join()

    
if __name__ == "__main__":
    main(sys.argv[1:])

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