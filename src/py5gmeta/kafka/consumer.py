from py5gmeta.common import identity
import sys
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.avro import AvroConsumer
from kafka import KafkaConsumer
import statistics
from prometheus_clientprometheus_client import start_http_server, Gauge
import proton
import gi
from gi.repository import Gst, GObject, GLib, GstApp, GstVideo
import requests
import avro.schema
from avro.io import DatumReader, BinaryDecoder
import io

gi.require_version('GLib', '2.0')
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')


gauge = Gauge(
    "application_latency",
    "Application Latency."
)

latencies = [0,0,0,0,0,0,0,0,0,0]


def get_topic(url, realm_name, client_id, client_secret, username, password, quad):

    token = identity.get_header_with_token(url, realm_name, client_id, client_secret, username, password)
    r = requests.post(url, headers=token)

    if r.status_code != 200:
        print(r.text)
        exit()
    topic = r.text
    return topic


def insert_topic(message):
    topic = input(message)
    return topic


def get_avro_consumer(topic, cloud_platform_host, bootstrap_port, registry_port):
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


def add_latency(value):
    if len(latencies) >= 10:
        latencies.pop(0)  # Remove the first element
    latencies.append(value)


def consume(avro, latencies, poll, topic, function):

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

def process_with_latencies(msg_sd):
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
    except:
        print("An error decoding the message happened!")
        
    outfile.close()



def decode(reader, msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


#consumer = KafkaConsumer(bootstrap_servers=platform_ip + ':' + kafka_port, auto_offset_reset='earliest')

def process_video(consumer, msg_sd, raw_sd, topic):
    appsrc = None
    pts = 0  # buffers presentation timestamp
    duration = 10 ** 9 / (10 / 1)  # frame duration
    framerate = '10.0'
    framerate_aux = '10.0'

    pipeline = None
    bus = None
    message = None

    # topic = 'video'

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

    video_buffer = msg_sd.body

    print("Received frame Content-Type: video/x-h264 of size {size}".format(size=len(raw_sd)))

    # READ FROM VIDEO TOPIC IGNORING DATAFLOW API TOPIC
    # print("\t Msg Source:" + videoparams['sender_id'] + " Size:" + str(len(video_buffer)) + " Header Size:" + videoparams['body_size'])
    for element in video_params:
        if element['key'] == "body_size":
            print("\t Msg Size:" + str(len(video_buffer)) + " Header Size:" + element['value'])
        if element['key'] == "dataSampleRate":
            print("\t Framerate:" + element['value'])
            framerate_aux = element['value']
            duration = 10 ** 9 / int(float(element['value']) / 1.0)  # frame duration
        if element['key'] == "sourceId":
            print("\t Msg Source:" + element['value'])
            # USE THE TARGET ID TO CONSUME JUST THAT VIDEO STREAM
            # if element['value'] == '21':
            if element['value'] == str(source_id):
                decodeFlag = True

    if decodeFlag:
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
    time.sleep(1.0 / float(framerate))

    # free resources
    pipeline.set_state(Gst.State.NULL)



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