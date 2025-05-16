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

