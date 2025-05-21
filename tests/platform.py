from unittest import TestCase
from proton.reactor import Container

class FiveGMetaPlatformTestCase(TestCase):
    def setUp(self):
        self.REQUESTS = ["Twas brillig, and the slithy toves",
            "Did gire and gymble in the wabe.",
            "All mimsy were the borogroves,",
            "And the mome raths outgrabe."]
        self.address = ""
        self.latitude    = 43.3128
        self.longitude    = -1.9750
        self.DB_USER_user="dbuser"
        self.DB_PASSWORD="dbpassword"
        self.DB_HOST="cloudplatform.francecentral.cloudapp.azure.com"
        self.DB__PORT="3307"
        self.tile=""
        self.username="5gmeta-platform"
        self.password="5gmeta-platform"
        self.send = False
        self.dataflowId = -1
        self.topic = "platform-unittestts"
        self.timeinterval=10
        self.keepAliveInterval=25 # time in seconds to send a new keep alive
        self.messages=1
        self.folder_path="images"
        self.dir_path = r'images'
        self.broker_address = "akkodismec.francecentral.cloudapp.azure.com"
        self.bootstrap_port = "31090"
        self.registry_port =  "31081"
        self.disable_instance_api = False


    def test_username(self):
        pass

    def test_password(self):
        pass

    def test_tile(self):
        pass



