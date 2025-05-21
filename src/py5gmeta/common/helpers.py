import json
from py5gmeta.common import api, identity
import re
import sys

def produce_or_consume():
    print(f"\nDo you want to consume data or produce an event?")
    print(f"For consuming data enter 'c' or 'C', for producing an event enter 'e' or 'E'")
    choice = ""
    while True:
        choice = input("Enter your choice: ")
        if choice == 'c' or choice == 'C' or choice == 'e' or choice == 'E':
            break
    return choice

def insert_tiles(available_tiles):
    print(f"\nYou have data available in the following tiles:")
    print(f"{available_tiles}")
    print(f"\nPlease enter the tiles where you want to consume data\nWhen done enter 'q' or 'Q'")
    tiles = []
    while True:
        tile = input("Tile: ")
        if tile == 'q' or tile == 'Q':
            break
        else:
            exists = False
            for x in available_tiles:
                if x == tile or x.startswith(tile):
                    exists = True
            if exists:
                tiles.append(tile)
            else:
                print(f"There is not data available in tile {tile}")
    return tiles

def insert_datatypes(avalaible_datatypes, tile):
    datatypes = []
    print(f"\nPlease enter the datatype you want to consume in tile {tile}\nWhen done enter 'q' or 'Q'")
    while True:
        datatype = input("Datatype: ")
        if datatype == 'q' or datatype == 'Q':
            break
        else:
            if datatype in avalaible_datatypes:
                datatypes.append(datatype)
            else:
                print(f"There is no {datatype} datatype available in tile {tile}")

    print(f"\nSelected datatype(s): {datatypes}\n")
    return  datatypes

def get_tiles_by_choice(choice, available_tiles):
    tiles = []
    if choice == 'c' or choice == 'C':
        # REQUEST TILES WITH DATA AVAILABLE
        tiles = insert_tiles(available_tiles)
    if choice == 'e' or choice == 'E':
        print(f"\nPlease enter the tiles where you want to produce events\nWhen done enter 'q' or 'Q'")
        while True:
            tile = input("Tile: ")
            if tile == 'q' or tile == 'Q':
                break
            else:
                tiles.append(tile)
    return tiles

def get_instance_types():
    pass


def get_topics(url, auth_header, username,  choice,  disable_instance_api, tiles):
    topics = []
    filters = ""
    source_ids = {}
    instance_ids = {}

    for tile in tiles:
        if choice == 'c' or choice == 'C':
            datatypes = []
            avalaible_datatypes = api.get_datatype_from_tile(url, auth_header, tile)
            avalaible_datatypes = list(filter(None, re.split('\[|\]|\"|,|\n|\s', avalaible_datatypes)))
            print(f"You have the following datatypes in tile {tile}: ")
            print(f"{avalaible_datatypes}")
            if not avalaible_datatypes:
                print(f"\nSorry, there are no datatypes available in tile {tile}\n")
            else:
                datatypes = insert_datatypes(avalaible_datatypes, tile)
                mec_id = str(api.get_mec_id(url, auth_header, tile))

                # REQUEST TOPICS TO DATAFLOW API
                for datatype in datatypes:
                    # REQUEST INSTANCES TO INSTANCE API
                    if not disable_instance_api:
                        instance_ids[mec_id] = []
                        avalaible_types_wide = api.get_types(url, auth_header, mec_id)
                        avalaible_types_json = json.loads(avalaible_types_wide)
                        avalaible_types = [d['type_name'] for d in avalaible_types_json]
                        print(f"You have the following instance types in tile {tile}: ")
                        print(f"{avalaible_types_wide}")

                        instance_type = ""
                        while instance_type not in avalaible_types:
                            instance_type = input(
                                f"Please enter the instance type for '{datatype}' pipeline in tile {tile}: ")
                            if instance_type not in avalaible_types:
                                print(f"There is no '{instance_type}' instance type in tile {tile}")
                        data = '{"username": "' + username + '", "datatype": "' + datatype + '", "instance_type": "' + instance_type + '", }'
                        #TODO fix this
                        data['X_Userinfo'] = "username"
                        instance = api.request_instance(url, auth_header, mec_id, data)
                        try:
                            instance_id = instance['instance_id']
                            instance_ids[mec_id].append(instance_id)
                            print(f"\nSelected instance type for '{datatype}' pipeline: {instance_type}\n")
                        except:
                            print(instance)
                            sys.exit("Please try again with an instancetype with lower requirements")
                    else:
                        instance_type = "small"

                    print(f"You have the following subdatatypes in tile {tile}: ")
                    ids = api.get_ids(url, auth_header, tile, datatype,  "", instance_type, filters=filters)
                    avalaible_subdatatypes = set()
                    for mec_id in ids:
                        id_props = api.get_id_properties(url, auth_header, str(mec_id))
                        avalaible_subdatatypes.add(id_props['dataTypeInfo']['dataSubType'])
                    avalaible_subdatatypes = list(avalaible_subdatatypes)
                    print(f"{avalaible_subdatatypes}")
                    print(
                        f"\nPlease enter the subdatatype of {datatype} datatype you want to consume [q or Q to not select]: ")
                    subdatatype = input("Subdatatype: ")
                    filters = ""
                    if subdatatype == 'q' or subdatatype == 'Q':
                        filters = ""
                    else:
                        filters = "&dataSubType=" + subdatatype
                    topic = api.request_topic(url, auth_header, tile, datatype, subdatatype,
                                              instance_type, filters=filters)
                    topics.append(topic)

                    if datatype == "video":
                        ids = api.get_ids(auth_header, tile, datatype, subdatatype, filters=filters)
                        source_ids[tile] = ids
        if choice == 'e' or choice == 'E':
            instance_type = "noinstance"
            datatype = "event"
            subdatatype = ""
            topic = api.request_topic(url, auth_header,  tile, datatype, subdatatype, instance_type, filters="")
            topics.append(topic)

    return topics, source_ids, instance_ids


def print_topics_information(**kargs):
    available_tiles = api.get_tiles(kargs['url'], kargs['auth_header'])
    available_tiles = list(filter(None, re.split('\[|\]|\"|,|\n|\s', available_tiles)))
    tiles = get_tiles_by_choice(kargs['choice'], available_tiles)
    print(f"\nSelected tile(s): {tiles}\n")
    topics, source_ids, instance_ids = get_topics(kargs['api_endpoint'], kargs['auth_header'],
                                                          kargs['username'], kargs['choice'], kargs['disable_instance_api'], tiles)
    if topics:
        print(f"Connect to the following kafka topics: {topics}")
        print(f"Kafka broker address: {kargs['platform_url']}")
        print(f"Kafka bootstrap port: {kargs['bootstrap_port()']}")
        print(f"Kafka schema registry port: {kargs['registry_port()']}")
        if source_ids:
            print(f"Source IDs for selecting the individual video flows: {source_ids}")

        while True:
            quit = input("\nTo stop using the platform please enter 'q' or 'Q': ")
            if quit == 'q' or quit == 'Q':
                print(f"\nExiting...")
                # TODO add a method to this class to return the token.
                auth_header = identity.get_header_with_token(kargs['identity_url'],
                                                             kargs['realm_name'], kargs['client_id'],
                                                             kargs['client_secret'], kargs['username'],
                                                             kargs['password'])
                choice = kargs['choice']
                if choice == 'c' or choice == 'C':
                    if not kargs['disable_instance_api']:
                        for mec_id in instance_ids.keys():
                            for instance_id in instance_ids[mec_id]:
                                api.delete_instance(kargs['api_endpoint'], kargs['auth_header'], mec_id,
                                                    instance_id)
                for topic in topics:
                    api.delete_topic(kargs['api_endpoint'], kargs['auth_header'], topic)
                sys.exit("\nThank you for using 5GMETA Platform. Bye!")
            else:
                pass
    else:
        sys.exit("You did not request any data. Thank you for using 5GMETA Platform. Bye!")

def msg_to_dict(msg, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    # User._address must not be serialized; omit from dict
    return dict(
                correlation_id = msg.correlation_id,
                redelivered = msg.redelivered,
                reply_to = msg.reply_to,
                destination = msg.destination,
                message_id = msg.message_id,
                mode = msg.mode,
                type = msg.type,
                priority = msg.priority,
                payload = msg.payload,
                properties = msg.properties )

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return


    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
