import unittest
from py5gmeta.common import address


class AddressTestCase(unittest.TestCase):
    def setUp(self):
        self.tile = "123456789"
        self.host = ""
        self.port = ""
        self.username = "testuser"
        self.password = "testuser"


    def test_get_amqp_broker_url(self):
      self.assertEqual(address.get_amqp_broker_url(self.host, self.port, self.username, self.password), "")

    def test_get_message_video_broker_url(self):
        address.get_message_video_broker_url()

    def test_get_url(self):
        address.get_url()

    def test_get_message_broker_address_by_tile(self):
        address.get_message_broker_address_by_tile(self.tile)


if __name__ == '__main__':
    unittest.main()
