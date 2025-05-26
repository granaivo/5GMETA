from __future__ import print_function
import base64
import json
import os
import unittest
from py5gmeta.activemq import amqp
from py5gmeta.common import message, address, geotile

class AMQPTestCase(unittest.TestCase):

    def setUp(self):
        self.body = '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'
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
        self.message_broker_host = "akkodismec.francecentral.cloudapp.azure.com"
        self.message_broker_port = "30672"
        self.registration_host = ""
        self.registration_port = ""
        self.video_broker_host = ""
        self.videoBroker_port = ""
        self.dataflowId = ""
        self.topic = "ampq-unittests"
        self.send = ""
        self.topics = ['newdataflow', 'terminatedataflow']
        # Getting the tiles from latitude and longitude.
        self.tile25 = geotile.get_tile_by_position(self.latitude, self.longitude, zoom=25)
        self.tile25 = geotile.get_tile_by_position(self.latitude, self.longitude, zoom=18)
        # Replace with your metadata
        self.dataflowmetadata = message.generate_metadata("video", "h265", 1.0, "video", "upload",  "Spain",  "Europe",  self.latitude,
                                                     self.longitude, 2, 2, 2, "vehicle")
        self.amqp_server_url = address.get_amqp_broker_url(self.message_broker_host, self.message_broker_port, self.username,
                                                 self.password)
        self.parameters_cits = {
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
        self.bodies = []
        self.cits_json_datasets_path = "../datasets/cits/json"
        if not self.bodies:
            cits_json_files = [cits_f for cits_f in os.listdir(self.cits_json_datasets_path) if cits_f.endswith('.json')]
            for cits_json_file in cits_json_files:
                with open(self.cits_json_datasets_path+ '/'+ cits_json_file) as f:
                    self.bodies.append(json.load(f))

        self.images = []


    def test_get_amqp_url(self):
        self.assertEqual(address.get_amqp_broker_url(self.message_broker_host, self.message_broker_port, self.username, self.password), "amqp://5gmeta-platform:5gmeta-platform@akkodismec.francecentral.cloudapp.azure.com:30672")

    def test_send_cits_body(self):
        #self.assertRaises(Exception, AMQP(subscription=self.topics, param=self.parameters) )
        amqp.send(self.amqp_server_url, self.topic, self.body)

    def test_send_cits_bodies(self):
        i = 0
        for body in self.bodies:
            i = i + 1
            content = message.messages_generator( "cits", 1,  body, ++i, tile=self.tile)
            amqp.send(self.amqp_server_url, self.topic+'-cits', content)

    def test_send_png_image(self):
        img_datasets_path = "../datasets/images"
        img_files = [img_f for img_f in os.listdir(img_datasets_path) if img_f.endswith('.png')]
        i = 0
        for img_f in img_files:
            i = i + 1
            with open(img_datasets_path + '/' + img_f, 'rb') as f:
                body =  base64.b64encode(f.read())
                content  = message.messages_generator("image", 1, None, i, tile=self.tile, image=img_datasets_path + '/' + img_f)
                amqp.send(self.amqp_server_url, self.topic+ '-image', content)


    def test_consume_cits_messages(self):
        amqp.receive(self.amqp_server_url, self.topic+'-cits', 10)


if __name__ == "__main__":
    unittest.main()
