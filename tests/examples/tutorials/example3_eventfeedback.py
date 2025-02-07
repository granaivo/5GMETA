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

