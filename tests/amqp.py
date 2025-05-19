from __future__ import print_function
import os
import unittest
from py5gmeta.common import message, address, geotile


class AMQPModuleTest(unittest.TestCase):
    def setUp(self):
        self.url =  ""
        self.device_type = "video_sensor"
        self.latitude = 43.295778
        self.longitude = -1.980823
        self.device_coord = "%s, %s" % (self.latitude, self.longitude)  # Lat, Lon

        self.server_url = "https://cloudplatform.francecentral.cloudapp.azure.com/"

        self.sourceType = os.getenv('VIDEO_SOURCE')
        self.sourcePar = os.getenv('VIDEO_PARAM')

        self.tile = geotile.get_tile_by_position(self.latitude, self.longitude, zoom=18)

        self.username = "5gmeta-platform"
        self.password = "5gmeta-platform"

        # Get Message Broker access
        self.service_name = "message-broker"
        self.auth_headers = ""
        self.url = "https://cloudplatform.francecentral.cloudapp.azure.com/api/v1"

        self.message_broker_host = "mecakkodis.francecentral.cloudapp.azure.com"
        self.message_broker_port = ""
        self.registration_host = ""
        self.registration_port = ""
        self.video_broker_host = ""
        self.videoBroker_port = ""
        self.dataflowId = ""
        self.topic = ""
        self.send = ""
        self.topics = ['newdataflow', 'terminatedataflow']
        # Getting the tiles from latitude and longitude.
        self.tile25 = geotile.get_tile_by_position(self.latitude, self.longitude, zoom=25)
        self.tile25 = geotile.get_tile_by_position(self.latitude, self.longitude, zoom=18)
        # Replace with your metadata
        self.dataflowmetadata = message.generate_metadata("video", "h265", "webrtc", "upload", "France", self.latitude,
                                                     self.longitude)
        self.server_url = address.get_amqp_broker_url(self.message_broker_host, self.message_broker_port, self.username,
                                                 self.password)
        self.parameters = {
            'deviceType': self.device_type,
            'serverURL': self.server_url,
            'vserverIP': self.video_broker_host,
            'vserverPort': self.videoBroker_port,
            'sourceType': self.sourceType,
            'sourcePar': self.sourcePar,
            'id': self.dataflowId,
            'tile': self.tile25,
            'metadata': self.dataflowmetadata,
            'send': self.send
        }


    def test_video_sensor(self):
        #self.assertRaises(Exception, AMQP(subscription=self.topics, param=self.parameters) )
        pass

    def test_get_amqp_url(self):
        self.assertEqual(address.get_amqp_broker_url(self.message_broker_host, self.message_broker_port, self.username, self.password), self.server_url)

