import json
import sys
import time
import proton
from confluent_kafka.admin import AdminClient, NewTopic
import unittest
from confluent_kafka import Producer, Consumer
from confluent_kafka.avro import AvroConsumer
from py5gmeta.kafka import prosumer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class Kafka(unittest.TestCase):

    def setUp(self):
        self.kafka_host = "cloudplatform.francecentral.cloudapp.azure.com"
        self.kafka_port = "31090"
        self.admin_client = AdminClient({'bootstrap.servers': self.kafka_host + ':' + self.kafka_port})


        self.tile = "031333123201"
        self.instance_type = "small"  # small
        self.platform_address = "cloudplatform.francecentral.cloudapp.azure.com"
        self.bootstrap_port = "31090"
        self.schema_registry_port = "443"
        self.platform_user = "5gmeta-platform"
        self.platform_password = "5gmeta-platform"
        self.group_id = "group1"
        self.topic = "5GMETA_1011_CITS_MEDIUM_34"
        self.new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in [self.topic]]
        self.registry_port = 443
        self.consumer = prosumer.create_consumer(self.platform_address, self.bootstrap_port, self.registry_port, self.group_id, self.topic)

        self.avro.subscribe([self.topic.upper()])
        self.p = Producer({'bootstrap.servers': 'cloudplatform.francecentral.cloudapp.azure.com:31090'})

        print("Subscibed topics: " + str(self.topic))
        print("Running...")

        self.i = 0
        # Trigger any available delivery report callbacks from previous produce() calls
        self.p.poll(1.0)

    def tearDown(self):
        self.avro.close()
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.p.flush()

    def test_create_topics(self):
        # Call create_topics to asynchronously create topics. A dict
        # of <topic,future> is returned.
        fs = self.admin_client.create_topics(self.new_topics)
        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))


    def test_produce(self):
       self.p.produce(self.topic, "TESTING".encode('utf-8'), callback=delivery_report)

    def test_consumer(self):
        c = prosumer.create_consumer(self.platform_address, self.bootstrap_port, self.registry_port, self.group_id, self.topic)

        c.subscribe([self.topic])

        msg = c.poll(1.0)
        print('Received message: {}'.format(msg.value().decode('utf-8')))

        c.close()

    def test_cits_consumer(self):
        msg = self.avro.poll(1.0)

        print("NEW MESSAGE")
        currentTime = time.time_ns() // 1_000_000

        sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
                         (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))

        # The AVRO Message here in mydata
        mydata = msg.value()  # .decode('latin-1') #.replace("'", '"')
        # print( "Message: " + str(mydata))
        # print(mydata['PROPERTIES'])

        # The QPID proton message: this is the message sent from the S&D to the MEC
        # print(mydata)
        raw_sd = mydata['BYTES_PAYLOAD']
        msg_sd = proton.Message()
        proton.Message.decode(msg_sd, raw_sd)

        # The msg_sd.body contains the data of the sendor
        bodyJson = json.loads(msg_sd.body)
        print("RECEIVED_TIME")
        print(currentTime)
        print("MESSAGE_TIMESTAMP")
        message_timestamp = mydata['MESSAGE_TIMESTAMP']
        print(message_timestamp)
        print("ORIGIN_TIME")
        origin_time = \
        bodyJson['cam']['camParameters']['lowFrequencyContainer']['basicVehicleContainerLowFrequency']['pathHistory'][
            0]['pathDeltaTime']
        print(origin_time)
        print("MESSAGE_ID")
        message_id = bodyJson['header']['stationID']
        print(message_id)
        latency1 = message_timestamp - origin_time
        print("LATENCY1 milliseconds")
        print(latency1)
        latency2 = currentTime - origin_time
        print("LATENCY2 milliseconds")
        print(latency2)


if __name__ == '__main__':
    unittest.main()