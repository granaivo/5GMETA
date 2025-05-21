from py5gmeta.common import identity, api
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import statistics
import proton
from avro.io import DatumReader, BinaryDecoder
import io
from uuid import uuid4
from py5gmeta.common.message import EventMessage
from py5gmeta.common.helpers import msg_to_dict, delivery_report
import sys


latencies = [0,0,0,0,0,0,0,0,0,0]
#consumer = KafkaConsumer(bootstrap_servers=platform_ip + ':' + kafka_port, auto_offset_reset='earliest')

def insert_topic(message):
    topic = input(message)
    return topic

def add_latency(value):
    if len(latencies) >= 10:
        latencies.pop(0)  # Remove the first element
    latencies.append(value)

def consume(avro, latencies, poll, topic, function):
    avro.subscribe([topic.upper()])
    print("Subscribed to topic: " + str(topic))
    print("Running...")
    while True:
        msg = avro.poll(poll)

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

        function(msg_sd, topic)
    avro.close()

def process_with_latencies(msg_sd, gauge):
    # The msg_sd.body contains the data of the sendor
        latency = time.time()*1000 - msg_sd.properties['timestamp']
        add_latency(latency)
        gauge.set(statistics.mean(latencies))
        print("Latency: "+ str(statistics.mean(latencies)))

def write_message(msg_sd,  i ):
    # The msg_sd.body contains the data of the sendor
    print(msg_sd.body)
    print("Size " + str(sys.getsizeof(msg_sd.body)))
    outfile = open("../output/body_"+str(i)+".txt", 'w')
    try:
        outfile.write(msg_sd.body)
    except IOError as e:
        print(e)
        
    outfile.close()


def decode(reader, msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


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


def create_producer():
    pass

def produce(producer , message, topic):
    print("Producing EventMessage records to topic {}. ^C to exit.".format(topic))
    producer.poll(0.0)
    try:
        msg_props = {"message": message}
        msg = EventMessage(properties=msg_props)
        producer.produce(topic=topic, key=str(uuid4()), value=msg, on_delivery=delivery_report)
    except ValueError:
        print("Invalid input, discarding record...")

    print("\\nFlushing records...")
    producer.flush()