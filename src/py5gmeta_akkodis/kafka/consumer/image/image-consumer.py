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
