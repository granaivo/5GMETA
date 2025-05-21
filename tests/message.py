import json
import string
import unittest
from  py5gmeta.common import message, database

class MessageTestCase(unittest.TestCase):
    def setUp(self):
        self.data_type = "cits"
        self.data_sub_type= "cam"
        self.data_sample_rate = 1.0
        self.dataformat = "json"
        self.latitude = 43.295778
        self.longitude = -1.980823
        self.direction = "upload"
        self.country = ""
        self.geo_limit = "Europe"
        self.source_time_zone = 2
        self.source_stratum_level = 1
        self.source_id = 1
        self.source_type = "Vehicle"
        self.name = "data_sub_type"
        self.body =  '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'


    def test_generate_meta_data(self):
        meta_data_info = message.generate_metadata(self.data_type, self.data_sub_type, self.data_sample_rate, self.dataformat, self.direction, self.country, self.geo_limit, self.latitude, self.longitude, self.source_time_zone, self.source_stratum_level, self.source_id, self.source_type)
        print(database.to_json(meta_data_info))

    def test_generate_random_group_id(self):
        pass

    def test_message_generator(self):
        pass

    def test_cits_messages_generator(self):
        pass

    def image_cits_messages_generator(self):
        pass

if __name__ == '__main__':
    unittest.main()