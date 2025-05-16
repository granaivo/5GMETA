
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
