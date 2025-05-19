from confluent_kafka.admin import AdminClient, NewTopic
import unittest
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})


def generateRandomGroupId(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


class Kafka(unittest.TestCase):

    def setUp(self):
        self.kafka_host = ""
        self.kafka_port = ""
        self.admin_client = AdminClient({'bootstrap.servers': self.kafka_host + ':' + self.kafka_port})
        self.new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["topic1", "topic2"]]

        self.tile = "031333123201"
        self.instance_type = "small"  # small
        self.platformaddress = "<ip>"
        self.bootstrap_port = "31090"
        self.schema_registry_port = "31081"
        self.platform_user = "<user>"
        self.platform_password = "<password>"
        self.group_id = "group1"
        self.topic = "5GMETA_1011_CITS_MEDIUM_34"

        self.avro = AvroConsumer({
            'bootstrap.servers': platformaddress + ':' + bootstrap_port,
            'schema.registry.url': 'http://' + platformaddress + ':' + schema_registry_port,
            'group.id': topic + '_' + generateRandomGroupId(4),
            'api.version.request': True,
            'auto.offset.reset': 'earliest'
        })

        self.avro.subscribe([topic.upper()])
        self.p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

        print("Subscibed topics: " + str(topic))
        print("Running...")

        i = 0
        # Trigger any available delivery report callbacks from previous produce() calls
        self.p.poll(0)

    def tearDown(self):
        self.avro.close()
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.p.flush()

    def test_consumer(self):
        c = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        })

        c.subscribe(['mytopic'])

        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))

        c.close()

    def test_cits_consumer(self):
        msg = self.avro.poll(1.0)

        if msg is None:
            # print("Empty msg: " + str(msg) );
            print(".", end="", flush=True)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
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
        '''print("Size " + str(sys.getsizeof(msg_sd.body)))

        outfile = open("../output/body_"+str(i)+".txt", 'w')
        i=i+1
        try:
            outfile.write(msg_sd.body)
        except:
            print("An error decoding the message happened!")

        outfile.close()
        '''


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

   def test_produce():
       # Asynchronously produce a message. The delivery report callback will
       # be triggered from the call to poll() above, or flush() below, when the
       # message has been successfully delivered or failed permanently.
       p.produce('mytopic', "TESTING".encode('utf-8'), callback=delivery_report)




def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
