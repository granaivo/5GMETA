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

# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generateRandomGroupId (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

if len(sys.argv) != 5:
    print("Usage: python3 cits-consumer.py topic platformaddress bootstrap_port registry_port ")
    exit()

topic=str(sys.argv[1])
platformaddress=str(sys.argv[2])
bootstrap_port=str(sys.argv[3])
schema_registry_port=str(sys.argv[4])

c = AvroConsumer({
    'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
    'schema.registry.url':'http://'+platformaddress+':' + schema_registry_port, 
    'group.id': topic+'_'+generateRandomGroupId(4),
    'api.version.request': True,
    'auto.offset.reset': 'earliest'
})

c.subscribe([topic.upper()])

print("Subscibed topics: " + str(topic))
print("Running...")

i = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        #print("Empty msg: " + str(msg) );
        print(".",  end="", flush=True)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    
    sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
        (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
    # The AVRO Message here in mydata
    mydata = msg.value() # .decode('latin-1') #.replace("'", '"')
    #print( "Message: " + str(mydata))
    print("NEW MESSAGE")
    #print(mydata['PROPERTIES'])

    # The QPID proton message: this is the message sent from the S&D to the MEC
    raw_sd = mydata['BYTES_PAYLOAD']
    msg_sd = proton.Message()
    proton.Message.decode(msg_sd, raw_sd)

    # The msg_sd.body contains the data of the sendor
    print(msg_sd.body)
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
# Some sample code for the cosumere can be find 
# here https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_consumer.py

from email import header
from confluent_kafka import Consumer, KafkaException
import sys
from pprint import pformat

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition
import sys
import base64
import sys

#from proton.handlers import MessagingHandler
import proton
import random
import string

# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generateRandomGroupId (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


if len(sys.argv) != 5:
    print("Usage: python3 image-consumer.py topic platformaddress bootstrap_port registry_port ")
    exit()


topic=str(sys.argv[1])
platformaddress=str(sys.argv[2])
bootstrap_port=str(sys.argv[3])
registry_port=str(sys.argv[4])



c = AvroConsumer({
    'bootstrap.servers': platformaddress+':'+bootstrap_port,
    'schema.registry.url':'http://'+platformaddress+':'+registry_port, 
    'group.id': topic+'_'+generateRandomGroupId(4),
    'api.version.request': True,
    'auto.offset.reset': 'earliest'
})


c.subscribe([topic.upper()])

print("Subscibed topics: " + str(topic))
print("Running...")

i = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        #print("Empty msg: " + str(msg) );
        print(".",  end="", flush=True)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    
    sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
        (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
    # The AVRO Message here in mydata
    mydata = msg.value() # .decode('latin-1') #.replace("'", '"')

    # The QPID proton message: this is the message sent from the S&D to the MEC
    raw_sd = mydata['BYTES_PAYLOAD']
    msg_sd = proton.Message()
    proton.Message.decode(msg_sd, raw_sd)

    # The msg_sd.body contains the data of the sendor
    #print("Size " + str(sys.getsizeof(msg_sd.body)))

    outfile = open("output/body_"+str(i)+".jpg", 'wb')
    i=i+1
    try:
        outfile.write(base64.b64decode(msg_sd.body))
    except:
        print("An error decoding the message happened!")
        
    outfile.close()

c.close()
import sys
import time
import numpy

import json

import gi

gi.require_version('GLib', '2.0')
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')

from gi.repository import Gst, GObject, GLib, GstApp, GstVideo

from kafka import KafkaConsumer

import ast
import requests
import proton

import avro.schema
from avro.io import DatumReader, BinaryDecoder
import io


def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


platform_ip = str(sys.argv[1])
kafka_port = str(sys.argv[2])
topic = str(sys.argv[3])
sourceId = str(sys.argv[4])


appsrc = None
pts = 0  # buffers presentation timestamp
duration = 10**9 / (10 / 1)  # frame duration
framerate = '10.0'
framerate_aux = '10.0'

pipeline = None
bus = None
message = None

#topic = 'video'
consumer = KafkaConsumer(bootstrap_servers=platform_ip+':'+kafka_port, auto_offset_reset='earliest')
consumer.subscribe([topic])

# initialize GStreamer
Gst.init(sys.argv[1:])

# build the pipeline
pipeline = Gst.parse_launch(
    'appsrc caps="video/x-h264, stream-format=byte-stream, alignment=au" name=appsrc ! h264parse config-interval=-1 ! decodebin ! videoconvert ! autovideosink'
)

appsrc = pipeline.get_by_name("appsrc")  # get AppSrc
# instructs appsrc that we will be dealing with timed buffer
appsrc.set_property("format", Gst.Format.TIME)

# instructs appsrc to block pushing buffers until ones in queue are preprocessed
# allows to avoid huge queue internal queue size in appsrc
appsrc.set_property("block", True)

# start playing
ret = pipeline.set_state(Gst.State.PLAYING)
if ret == Gst.StateChangeReturn.FAILURE:
    print("Unable to set the pipeline to the playing state.")
    exit(-1)

# wait until EOS or error
bus = pipeline.get_bus()

# READ FROM VIDEO TOPIC IGNORING DATAFLOW API TOPIC
schema = avro.schema.Parse(open("video-schema.avsc").read())
reader = DatumReader(schema)

# Parse message
while True:
    for message in consumer:
        decodeFlag = False
        msg = message.value
        msg_dict = decode(msg)
        
        # READ FROM VIDEO TOPIC IGNORING DATAFLOW API TOPIC
        videoparams = msg_dict['PROPERTIES']
        #print(type(videoparams))
        print(videoparams)
        # READ FROM VIDEO TOPIC IGNORING DATAFLOW API TOPIC
        raw_sd = msg_dict['BYTES_PAYLOAD']
        msg_sd = proton.Message()
        proton.Message.decode(msg_sd, raw_sd)

        video_buffer = msg_sd.body

        print("Received frame Content-Type: video/x-h264 of size {size}".format(size=len(raw_sd)))

        # READ FROM VIDEO TOPIC IGNORING DATAFLOW API TOPIC
        # print("\t Msg Source:" + videoparams['sender_id'] + " Size:" + str(len(video_buffer)) + " Header Size:" + videoparams['body_size'])
        for element in videoparams:
            if element['key'] == "body_size":
                print("\t Msg Size:" + str(len(video_buffer)) + " Header Size:" + element['value'])
            if element['key'] == "dataSampleRate":
                print("\t Framerate:" + element['value'])
                framerate_aux = element['value']
                duration = 10**9 / int(float(element['value']) / 1.0)  # frame duration
            if element['key'] == "sourceId":
                print("\t Msg Source:" + element['value'])
                # USE THE TARGET ID TO CONSUME JUST THAT VIDEO STREAM
                #if element['value'] == '21':
                if element['value'] == str(sourceId):
                    decodeFlag = True

        if decodeFlag :
            print("DECODE ON!")
            framerate = framerate_aux
            gst_buffer = Gst.Buffer.new_allocate(None, len(video_buffer), None) 
            gst_buffer.fill(0, video_buffer)

            # set pts and duration to be able to record video, calculate fps
            pts += duration  # Increase pts by duration
            gst_buffer.pts = pts
            gst_buffer.duration = duration

            # emit <push-buffer> event with Gst.Buffer
            appsrc.emit("push-buffer", gst_buffer)
    time.sleep(.1)
    time.sleep(1.0/float(framerate))

# free resources
pipeline.set_state(Gst.State.NULL)


from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import requests
import security


def get_topic(platform_address, registry_port, platform_user, platform_password, tile, instance_type):
    """Get the Kafka topic for the given tile and instance type."""
    headers = security.get_header_with_token(platform_user, platform_password)
    url = f"http://5gmeta-platform.eu/dataflow-api/topics/cits/query?dataSubType=json&quadkey={tile}&instance_type={instance_type}"
    return requests.post(url, headers=headers).text


def create_consumer(platform_address, bootstrap_port, registry_port, group_id, topic):
    """Create a Kafka consumer for the given topic."""
    schema_registry_conf = {'url': f"http://{platform_address}:{registry_port}"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)
    return DeserializingConsumer({
        'bootstrap.servers': f"{platform_address}:{bootstrap_port}",
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer
    })


def consume_messages(consumer):
    """Consume messages from the given Kafka consumer."""
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print(".", end="", flush=True)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        try:
            key = msg.key()
            value = msg.value()
            print("Message: key={}, value={}".format(key, value))
        except:
            print(f"Message deserialization failed")
            continue


if __name__ == '__main__':
    # Configuration
    tile = "031333123201"
    instance_type = "small" #small
    platform_address = "<ip>"
    bootstrap_port = "31090"
    registry_port = "31081"
    platform_user = "<user>"
    platform_password = "<password>"
    group_id = "group1"

    # Get the Kafka topic
    topic = get_topic(platform_address, registry_port, platform_user, platform_password, tile, instance_type)

    # Create the Kafka consumer
    consumer = create_consumer(platform_address, bootstrap_port, registry_port, group_id, topic)

    # Subscribe to the topic and consume messages
    consumer.subscribe([topic.upper()])
    print(f"Subscribed topics: {str(topic)}")
    print("Running...")
    consume_messages(consumer)

    # Close the Kafka consumer
    consumer.close()

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
import security

#from proton.handlers import MessagingHandler
import proton

tile="1202231113220102"

platformaddress = "<ip>" 
bootstrap_port = "31090"
registry_port =  "31081"


platformuser = "<user>"
platformpassword = "<password>"



headers=security.get_header_with_token(platformuser,platformpassword)


url = "http://5gmeta-platform.eu/dataflow-api/topics/cits/query?dataSubType=jpg&quadkey="+tile

# The request returns the generated topic
topic = requests.post(url, headers=headers).text
print(topic)
c = AvroConsumer({
    'bootstrap.servers': platformaddress+':'+bootstrap_port,
    'schema.registry.url':'http://'+platformaddress+':'+registry_port, 
    'group.id': 'group1',
    'api.version.request': True,
    'auto.offset.reset': 'earliest'
})

#topics = ["image"] #, "colors_filtered", "colors"]

c.subscribe([topic.upper()])
#c.subscribe(topics)

print("Subscibed topics: " + str(topic))
print("Running...")

i = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        #print("Empty msg: " + str(msg) );
        print(".",  end="", flush=True)
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    
    sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
        (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    
    # The AVRO Message here in mydata
    mydata = msg.value() # .decode('latin-1') #.replace("'", '"')
    #print( "Message: " + str(mydata))

    # The QPID proton message: this is the message sent from the S&D to the MEC
    print(mydata['PROPERTIES'])
    raw_sd = mydata['BYTES_PAYLOAD']
    msg_sd = proton.Message()
    proton.Message.decode(msg_sd, raw_sd)

    # The msg_sd.body contains the data of the sendor
    # print(msg_sd.body)
    print("Size " + str(sys.getsizeof(msg_sd.body)))

    outfile = open("output/body_"+str(i)+".jpg", 'wb')
    i=i+1
    try:
        outfile.write(base64.b64decode(msg_sd.body))
    except:
        print("An error decoding the message happened!")
        
    outfile.close()

    # TEST https://stackoverflow.com/questions/40059654/python-convert-a-bytes-array-into-json-format/40060181
    #print('Received message: {} \n'.format(msg.value().decode('latin-1'))) #'utf-8')))
    # Load the JSON to a Python list & dump it back out as formatted JSON
    # print(" \n")
    #data = json.loads(mydata)
    # s = json.dumps(mydata, indent=4, sort_keys=True)
    # print(s)

c.close()
