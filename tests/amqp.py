from __future__ import print_function
from proton.handlers import MessagingHandler
from proton.reactor import Container
import unittest
import sys
from  py5gmeta.activemq import amqp
from  py5gmeta.common import api, address
import gi

gi.require_version('GLib', '2.0')
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')

from gi.repository import Gst, GObject, GLib, GstApp, GstVideo
import os
import unittest
from pygeotile.tile import Tile
import sys, json, requests, random, string

from amqp_manager import AMQP
import unittest
from py5gmeta.activemq import amqp
from py5gmeta.common import api, message, address, geotile

class AMQP(unittest.TestCase):
    def setUp(self):
        self.url =  ""
        self._messages_actually_received = 0
        self._stopping = False
        self.deviceType = "video_sensor"

        # Geoposition - Next steps: from GPS device. Now hardcoded.
        self.latitude = 43.295778
        self.longitude = -1.980823
        self.deviceCoord = "%s, %s" % (latitude, longitude)  # Lat, Lon

        # Server URL to get the MEC information from 5GMETA Cloud Infrastructure
        self.server_url = ""

        self.sourceType = os.getenv('VIDEO_SOURCE')
        self.sourcePar = os.getenv('VIDEO_PARAM')
        latitude = 43.2952
        self.longitude = -1.9850

        self.tile = geotile.get_tile_by_position(latitude, longitude, zoom=18)

        self.username = "5gmeta-platform"
        self.password = "5gmeta-platform"

        # Get Message Broker access
        self.service_name = "message-broker"
        self.auth_headers = ""
        self.url = "https://cloudplatform.francecentral.cloudapp.azure.com/api/v1"

        message_broker_host, message_broker_port = api.discover_sb_service(url, tile, service_name, auth_headers)
        url = "amqp://" + username + ":" + password + "@" + message_broker_host + ":" + str(
            message_broker_port) + "/topic://event"

    def test_video(self):
        appsrc = None

        pipeline = None
        bus = None
        message = None

        pts = 0  # buffers presentation timestamp

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
        print("Pipeline Playing")
        ret = pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            print("Unable to set the pipeline to the playing state.")
            exit(-1)

        print("Container RECEIVER")
        Container(amqp.Receiver(address.url)).run()
        # wait until EOS or errr
        bus = pipeline.get_bus()
        # free resources
        pipeline.set_state(Gst.State.NULL)


    def test_on_message(self, event):
        global pts
        global duration

        if self._stopping:
            return

        print(event.message.properties['body_size'])
        print(
            "Received frame Content-Type: video/x-h264 of size {size}".format(size=event.message.properties['body_size']))
        # Change this number (54) with the Id you wand to debug from logs
        if (event.message.properties['sourceId'] == 54):
            gst_buffer = Gst.Buffer.new_allocate(None, int(event.message.properties['body_size']), None) 
            gst_buffer.fill(0, event.message.body)

            # set pts and duration to be able to record video, calculate fps
            framerate = float(event.message.properties['dataSampleRate'])
            duration = 10**9 / (int(framerate / 1.0))  # frame duration

            pts += duration  # Increase pts by duration
            gst_buffer.pts = pts
            gst_buffer.duration = duration

            # emit <push-buffer> event with Gst.Buffer
            appsrc.emit("push-buffer", gst_buffer)

        self._messages_actually_received += 1
        #event.connection.close()
        #self._stopping = True


    def test_video_sendor(self):
        # Getting the tiles from latitude and longitude.
        tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=25)
        print('(Latitude: %s, Longitude: %s) | QuadTree: %s' % (latitude, longitude, str(tileTmp.quad_tree)))
        tile25 = str(tileTmp.quad_tree)

        tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
        print('(Latitude: %s, Longitude: %s) | QuadTree: %s' % (latitude, longitude, str(tileTmp.quad_tree)))
        tile18 = str(tileTmp.quad_tree)

        # Replace with your metadata
        dataflowmetadata = {
            "dataTypeInfo": {
                "dataType": "video",
                "dataSubType": "h264"
            },
            "dataInfo": {
                "dataFormat": "webrtc",
                "dataSampleRate": 10.0,
                "dataflowDirection": "upload",
                "extraAttributes": ""
            },
            "licenseInfo": {
                "licenseGeolimit": "europe",
                "licenseType": "profit"
            },
            "dataSourceInfo": {
                "sourceTimezone": 2,
                "sourceStratumLevel": 1,
                "sourceId": 12345678,
                "sourceType": "vehicle",
                "sourceLocationInfo": {
                    "locationQuadkey": tile18,
                    "locationCountry": "ESP",
                    "locationLatitude": latitude,
                    "locationLongitude": longitude
                }
            }
        }

        # Get Message Broker access
        service = "message-broker"
        messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile18, service)
        if messageBroker_ip == -1 or messageBroker_port == -1:
            print(service + " service not found")
            exit(-1)

        # Register a New stream into the System
        service = "registration-api"
        Registration_ip, Registration_port = discovery_registration.discover_sb_service(tile18, service)
        if Registration_ip == -1 or Registration_port == -1:
            print(service + " service not found")
            exit(-1)

        # Get Video Broker access
        service = "video-broker"
        videoBroker_ip, videoBroker_port = discovery_registration.discover_sb_service(tile18, service)
        if videoBroker_ip == -1 or videoBroker_port == -1:
            print(service + " service not found")
            exit(-1)

        # Get Topic and dataFlowId to push data into the Message Broker
        dataflowId, topic, send = discovery_registration.register(dataflowmetadata, tile18)

        print("\n\tREGISTRATION DONE\n")

        server_url = "amqp://" + username + ":" + password + "@" + messageBroker_ip + ":" + str(
            messageBroker_port) + "/topic://"

        # Prepare all the AMQP parameters to proceed
        parameters = {
            'deviceType': deviceType,
            'serverURL': server_url,
            'vserverIP': videoBroker_ip,
            'vserverPort': videoBroker_port,
            'sourceType': sourceType,
            'sourcePar': sourcePar,
            'id': dataflowId,
            'tile': tile25,
            'metadata': dataflowmetadata,
            'send': send
        }

        topics = ['newdataflow', 'terminatedataflow']
        try:
            source = AMQP(subscription=topics, param=parameters)
        except:
            print('Error running webrtc_proxy')
            raise Exception('Error running webrtc_proxy')


    def test_send_sensor_devices(self):
        parser = optparse.OptionParser(usage="usage: %prog [options]",
                                       description="Send messages to the supplied address.")

        parser.add_option("-a", "--address",
                          help="address to which messages are sent (default %default)")

        parser.add_option("-m", "--messages", type="int", default=100,
                          help="number of messages to send (default %default)")

        parser.add_option("-t", "--timeinterval", default=10,
                          help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")

        opts, args = parser.parse_args()

        # Get Message Broker access
        service = "message-broker"
        messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile, service)
        if messageBroker_ip == -1 or messageBroker_port == -1:
            print(service + " service not found")
            exit(-1)

        # Get Topic and dataFlowId to push data into the Message Broker
        dataflowId, topic = discovery_registration.register(dataflowmetadata, tile)

        # Insert data into Sensor and Device database
        connection, dataflows, produced, owner, sensor, internalSensor = sd_database.prepare_database(db_user,
                                                                                                      db_password,
                                                                                                      db_ip, db_port)
        sd_database.insert_dataflow_localdb(dataflowmetadata, connection, dataflows, owner, dataflowId)
        sd_database.insert_sensor_local_db(dataflowmetadata, connection, sensor)
        sd_database.insert_internal_sensor_local_db(connection, internalSensor)
        sd_database.insert_dataflow_produced_dataflows_local_db(connection, produced, dataflowId)

        opts.address = "amqp://" + username + ":" + password + "@" + messageBroker_ip + ":" + str(
            messageBroker_port) + ":/topic://" + topic

        jargs = json.dumps(args)

        print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(
            opts.address) + "\n")

        # Generate message
        content.messages_generator(opts.messages, tile)

        # send message
        while (True):
            try:
                Container(Sender(opts.address, content.messages)).run()
                sd_database.keepAliveDataflow(db_ip, db_user, db_password, db_port, tile)
                print("... \n")
            except KeyboardInterrupt:
                pass
            if (int(opts.timeinterval) == 0):
                break
            time.sleep(int(opts.timeinterval))


    def test_sensor_devices(self):
        parser = optparse.OptionParser(usage="usage: %prog [options]",
                                       description="Send messages to the supplied address.")

        parser.add_option("-a", "--address",
                          help="address to which messages are sent (default %default)")

        parser.add_option("-m", "--messages", type="int", default=1,
                          help="number of messages to send (default %default)")

        parser.add_option("-t", "--timeinterval", default=10,
                          help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")

        # Get Message Broker access
        service = "message-broker"
        messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile, service)
        if messageBroker_ip == -1 or messageBroker_port == -1:
            print(service + " service not found")
            exit(-1)

        # Get Topic and dataFlowId to push data into the Message Broker
        dataflowId, topic = discovery_registration.register(dataflowmetadata, tile)

        print("dataflow id is " + str(dataflowId))

        # Insert data into Sensor and Device database
        connection, dataflows, produced, owner, sensor, internalSensor = sd_database.prepare_database(db_user,
                                                                                                      db_password,
                                                                                                      db_ip, db_port)
        sd_database.insert_dataflow_localdb(dataflowmetadata, connection, dataflows, owner, dataflowId)
        sd_database.insert_sensor_local_db(dataflowmetadata, connection, sensor)
        sd_database.insert_internal_sensor_local_db(connection, internalSensor)
        sd_database.insert_dataflow_produced_dataflows_local_db(connection, produced, dataflowId)

        opts, args = parser.parse_args()

        opts.address = "amqp://" + username + ":" + password + "@" + messageBroker_ip + ":" + str(
            messageBroker_port) + "/topic://" + topic

        jargs = json.dumps(args)

        print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(
            opts.address) + "\n")

        # generate message
        content.messages_generator(opts.messages, tile)

        # send message
        while (True):
            try:
                Container(Sender(opts.address, content.messages)).run()
                sd_database.keepAliveDataflow(db_ip, db_user, db_password, db_port, tile)
                print("... \n")
            except KeyboardInterrupt:
                pass
            if (int(opts.timeinterval) == 0):
                break
            time.sleep(int(opts.timeinterval))  #

    def test_video_sender(self):

        parser = optparse.OptionParser(usage="usage: %prog [options]",
                                       description="Send messages to the supplied address.")

        parser.add_option("-a", "--address",
                          help="address to which messages are sent (default %default)")

        parser.add_option("-m", "--messages", type="int", default=1,
                          help="number of messages to send (default %default)")

        parser.add_option("-t", "--timeinterval", default=10,
                          help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")

        # Get Message Broker access
        service = "message-broker"
        tile = "1202022213220223"
        messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile, service)
        if messageBroker_ip == -1 or messageBroker_port == -1:
            print(service + " service not found")
            exit(-1)

        # Get Topic and dataFlowId to push data into the Message Broker
        dataflowId, topic, send = discovery_registration.register(dataflowmetadata, tile)

        print("dataflow id is " + str(dataflowId))

        opts, args = parser.parse_args()
        messageBroker_port = 31888
        opts.address = "amqp://" + username + ":" + password + "@" + messageBroker_ip + ":" + str(
            messageBroker_port) + "/topic://" + topic

        jargs = json.dumps(args)

        print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(
            opts.address) + "\n")

        # generate message
        content.messages_generator(opts.messages, tile)

        # send message
        while (True):
            try:
                if (send):
                    print("Send received, send data")
                    Container(Sender(opts.address, content.messages)).run()
                    discovery_registration.keepAliveDataflow(dataflowmetadata, dataflowId)

                print("... \n")
            except KeyboardInterrupt:
                pass
            if (int(opts.timeinterval) == 0):
                break
            time.sleep(int(opts.timeinterval))

    def test_broker_host(self):
        self.assertEqual(message_broker_host, "akkodis.francecentral.cloudapp.azure.com")

    def test_broker_port(self):
        self.assertEqual(message_broker_port, "30162")

    def test_get_amqp_url(self):
        self.assertEqual(address.get_amqp_broker_url(message_broker_host, message_broker_port, username, password), url)

    def test_get_tile(self):
        pass