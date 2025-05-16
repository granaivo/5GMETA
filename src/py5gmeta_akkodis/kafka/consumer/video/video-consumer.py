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


