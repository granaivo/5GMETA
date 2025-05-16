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
