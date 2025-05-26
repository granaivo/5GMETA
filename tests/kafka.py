import json
import sys
import time
import unittest
import proton
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from py5gmeta.kafka import prosumer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class KafkaTestCase(unittest.TestCase):

    def setUp(self):
        self.kafka_host = "192.168.49.2"
        self.kafka_port = "31090"
        self.admin_client = AdminClient({'bootstrap.servers': self.kafka_host + ':' + self.kafka_port})
        self.tile = "031333123201"
        self.instance_type = "small"  # small
        self.platform_address = "192.168.49.2"
        self.bootstrap_port = "31090"
        self.platform_user = "5gmeta-platform"
        self.platform_password = "5gmeta-platform"
        self.group_id = "group10"
        self.topic = "5GMETA_1011_CITS_MEDIUM_34"
        self.new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in [self.topic]]
        self.registry_port = 31081
        self.consumer = prosumer.create_consumer(self.platform_address, self.bootstrap_port, self.registry_port, self.group_id, self.topic)
        self.cits_consumer = prosumer.create_consumer(self.platform_address, self.bootstrap_port, self.registry_port,
                                                 self.group_id, self.topic)
        self.consumer.subscribe([self.topic.upper()])
        self.producer = Producer({'bootstrap.servers': '192.168.49.2:31090'})

        print("Subscibed topics: " + str(self.topic))
        print("Running...")

        self.i = 0
        # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(1.0)

    def tearDown(self):
        self.consumer.close()
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.producer.flush()

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
       self.producer.produce(self.topic, "TESTING".encode('utf-8'), callback=delivery_report)

    def test_consumer(self):
        self.consumer.subscribe([self.topic])
        msg = self.consumer.poll(10.0)
        print(msg)
        print('Received message: {}'.format(msg.value().decode('utf-8')))


    def test_cits_consumer(self):
        self.cits_consumer.subscribe([self.topic])
        msg = self.cits_consumer.poll(60.0)
        print(msg)
        print("NEW MESSAGE")
        currentTime = time.time_ns() // 1_000_000
        sys.stderr.write('\n%% %s [%d] at offset %d with key %s:\n\n' %
                         (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
        # The AVRO Message here in mydata
        mydata = msg.value()  # .decode('latin-1') #.replace("'", '"')
        print(mydata)
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