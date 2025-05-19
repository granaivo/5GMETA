from uuid import uuid4
from avro_to_python.reader import AvscReader
from py5gmeta.common.message import EventMessage
from py5gmeta.common.helpers import msg_to_dict, delivery_report

import sys


def produce(producer , message, topic):
    print("Producing EventMessage records to topic {}. ^C to exit.".format(topic))
    producer.poll(0.0)
    try:
        msg_props = {"message": message}
        msg = EventMessage(properties=msg_props)

        producer.produce(topic=topic, key=str(uuid4()), value=msg, on_delivery=delivery_report)

    except ValueError:
        print("Invalid input, discarding record...")

    print("\\nFlushing records...")
    producer.flush()
