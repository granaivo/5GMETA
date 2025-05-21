from __future__ import print_function
import os
import unittest
from py5gmeta.common import message, address, geotile, database
from py5gmeta.activemq import amqp


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


    def test_get_amqp_url(self):
        self.assertEqual(address.get_amqp_broker_url(self.message_broker_host, self.message_broker_port, self.username, self.password), "amqp://5gmeta-platform:5gmeta-platform@akkodismec.francecentral.cloudapp.azure.com:30672")

    def test_send(self):
        #self.assertRaises(Exception, AMQP(subscription=self.topics, param=self.parameters) )
        amqp.send(self.amqp_server_url, self.topic, self.body)

    def test_consume(self):
        amqp.receive(self.amqp_server_url, self.topic, 10)



