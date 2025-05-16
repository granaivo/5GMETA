from py5gmeta.common import keycloak
from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat
import time
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition
import sys
import base64
import requests
import proton
import os
import statistics
from prometheus_client import start_http_server, Gauge

platformaddress = os.environ['CLOUD_PLATFORM_HOST']
bootstrap_port = os.environ['KAFKA_BOOSTSTRAP_PORT']
username = ""
password = ""


url = "https://"+platformaddress+':'+dataflowapi_port+'/dataflow-api/topics/cits/query?instance_type=large&quadkey='+quad

print(requests.delete("https://"+platformaddress+':'+dataflowapi_port+'/dataflow-api/topics/5GMETA_1009_CITS_LARGE_7', headers=keycloak.get_header_with_token("5gmeta", "5Gm3t4!")))
     time.sleep(100000)


def get_topic(url, username, password, quad):

    token = keyclock.get_token(username, password)
    r = requests.post(url, headers=token)

    if r.status_code != 200:
        print(r.text)
        exit()
    topic = r.text
    return topic


def get_insert_topic(message)
    topic = input(message)
    return topic


def get_avro_consumer(topic, cloud_platform_host, bootstrp_port, registry_port):
    c = AvroConsumer({
        'bootstrap.servers': cloud_platform_host +':' + bootstrap_port,
        'schema.registry.url':'http://'+ cloud_platform_host +':' + registry_port, 
        'group.id': 'group1',
        'api.version.request': True,
        'auto.offset.reset': 'earliest'
    })

    c.subscribe([topic.upper()])

    print("Subscribed to topic: " + str(topic))
    print("Running...")
    return c



i = 0

monitoring_port = int(os.getenv("MONITORING_PORT", 8080))
start_http_server(monitoring_port)
gauge = Gauge(
    "application_latency",
    "Application Latency."
)

latencies = [0,0,0,0,0,0,0,0,0,0]
def add_latency(value):
    if len(latencies) >= 10:
        latencies.pop(0)  # Remove the first element
    latencies.append(value)

def consume(avro, latencies):

    while True:
        msg = c.poll(0.1)

        if msg is None:
            #print("Empty msg: " + str(msg) );
            print(".",  end="", flush=True)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        # The AVRO Message here in mydata
        mydata = msg.value() # .decode('latin-1') #.replace("'", '"')

        # The QPID proton message: this is the message sent from the S&D to the MEC
        raw_sd = mydata['BYTES_PAYLOAD']
        msg_sd = proton.Message()
        proton.Message.decode(msg_sd, raw_sd)

        # The msg_sd.body contains the data of the sendor
        latency = time.time()*1000 - msg_sd.properties['timestamp']
        add_latency(latency)
        gauge.set(statistics.mean(latencies))
        print("Latency: "+ str(statistics.mean(latencies)))

    avro.close()
