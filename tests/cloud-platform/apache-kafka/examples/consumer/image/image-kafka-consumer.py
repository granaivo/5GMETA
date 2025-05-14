
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
