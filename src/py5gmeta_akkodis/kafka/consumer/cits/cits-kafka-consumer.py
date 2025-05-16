from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import requests
import security


def get_topic(platform_address, registry_port, platform_user, platform_password, tile, instance_type):
    """Get the Kafka topic for the given tile and instance type."""
    headers = security.get_header_with_token(platform_user, platform_password)
    url = f"http://5gmeta-platform.eu/dataflow-api/topics/cits/query?dataSubType=json&quadkey={tile}&instance_type={instance_type}"
    return requests.post(url, headers=headers).text


def create_consumer(platform_address, bootstrap_port, registry_port, group_id, topic):
    """Create a Kafka consumer for the given topic."""
    schema_registry_conf = {'url': f"http://{platform_address}:{registry_port}"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)
    return DeserializingConsumer({
        'bootstrap.servers': f"{platform_address}:{bootstrap_port}",
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer
    })


def consume_messages(consumer):
    """Consume messages from the given Kafka consumer."""
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print(".", end="", flush=True)
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        try:
            key = msg.key()
            value = msg.value()
            print("Message: key={}, value={}".format(key, value))
        except:
            print(f"Message deserialization failed")
            continue


if __name__ == '__main__':
    # Configuration
    tile = "031333123201"
    instance_type = "small" #small
    platform_address = "<ip>"
    bootstrap_port = "31090"
    registry_port = "31081"
    platform_user = "<user>"
    platform_password = "<password>"
    group_id = "group1"

    # Get the Kafka topic
    topic = get_topic(platform_address, registry_port, platform_user, platform_password, tile, instance_type)

    # Create the Kafka consumer
    consumer = create_consumer(platform_address, bootstrap_port, registry_port, group_id, topic)

    # Subscribe to the topic and consume messages
    consumer.subscribe([topic.upper()])
    print(f"Subscribed topics: {str(topic)}")
    print("Running...")
    consume_messages(consumer)

    # Close the Kafka consumer
    consumer.close()
