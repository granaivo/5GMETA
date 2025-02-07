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
    # Add any message you like from terminal | Or Define the message according to your data
    message = str(sys.argv[1])

    schema_str = """
{
	"type": "record",
	"name": "KsqlDataSourceSchema",
	"namespace": "io.confluent.ksql.avro_schemas",
	"fields": [
		{
			"name": "MESSAGE_TIMESTAMP",
			"type": [
				"null",
				"long"
			],
			"default": null
		},
		{
			"name": "CORRELATION_ID",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "REDELIVERED",
			"type": [
				"null",
				"boolean"
			],
			"default": null
		},
		{
			"name": "REPLY_TO",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "DESTINATION",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "MESSAGE_ID",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "MODE",
			"type": [
				"null",
				"int"
			],
			"default": null
		},
		{
			"name": "TYPE",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "PRIORITY",
			"type": [
				"null",
				"int"
			],
			"default": null
		},
		{
			"name": "BYTES_PAYLOAD",
			"type": [
				"null",
				"bytes"
			],
			"default": null
		},
		{
			"name": "PROPERTIES",
			"type": [
				"null",
				{
					"type": "array",
					"items": {
						"type": "record",
						"name": "KsqlDataSourceSchema_PROPERTIES",
						"fields": [
							{
								"name": "key",
								"type": [
									"null",
									"string"
								],
								"default": null
							},
							{
								"name": "value",
								"type": [
									"null",
									"string"
								],
								"default": null
							}
						],
						"connect.internal.type": "MapEntry"
					},
					"connect.name": "io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_PROPERTIES"
				}
			],
			"default": null
		}
	],
	"connect.name": "io.confluent.ksql.avro_schemas.KsqlDataSourceSchema"
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
