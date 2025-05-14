from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from avro_to_python.reader import AvscReader
from event_message import EventMessage
from helpers import msg_to_dict, delivery_report
from config import TOPIC, BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL

import sys

def main():

    if len(sys.argv) != 2:
        print("Usage: python3 kafka_event_sender.py message")
        exit()

    message = str(sys.argv[1])

    schema_str = """
    {
        "connect.name": "com.datamountaineer.streamreactor.connect.jms",
        "fields": [
            {
                "default": null,
                "name": "message_timestamp",
                "type": [
                    "null",
                    "long"
                ]
            },
            {
                "default": null,
                "name": "correlation_id",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "redelivered",
                "type": [
                    "null",
                    "boolean"
                ]
            },
            {
                "default": null,
                "name": "reply_to",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "destination",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "message_id",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "mode",
                "type": [
                    "null",
                    "int"
                ]
            },
            {
                "default": null,
                "name": "type",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "priority",
                "type": [
                    "null",
                    "int"
                ]
            },
            {
                "default": null,
                "name": "payload",
                "type": [
                    "null",
                    "string"
                ]
            },
            {
                "default": null,
                "name": "properties",
                "type": [
                    "null",
                    {
                        "type": "map",
                        "values": [
                            "null",
                            "string"
                        ]
                    }
                ]
            }
        ],
        "name": "jms",
        "namespace": "com.datamountaineer.streamreactor.connect",
        "type": "record"
    }
    """
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer( schema_registry_client,
                                      schema_str,
                                      msg_to_dict )

    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing EventMessage records to topic {}. ^C to exit.".format(TOPIC))
    producer.poll(0.0)
    try:
        msg_props = {"message": message}
        msg = EventMessage(properties=msg_props)

        producer.produce(topic=TOPIC, key=str(uuid4()), value=msg, on_delivery=delivery_report)

    except ValueError:
        print("Invalid input, discarding record...")

    print("\\nFlushing records...")
    producer.flush()

if __name__ == '__main__':
    main()
