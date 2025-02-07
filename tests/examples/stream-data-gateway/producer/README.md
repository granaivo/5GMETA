# Event Producer examples
This is a Producer of events to be broadcasted to 5GMETA Platform

# Required Packages
Install these packages using ***pip*** package manager
- ```pip install confluent-kafka==2.0.2```
- ```pip install avro```
- ```pip install avro-python3```
- ```pip install fastavro```
- ```pip install avro_to_python```

# Usage
The following examples must be used in combination of [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client) that returns apropriate topic and ip and ports to be run.

-   *KAFKA Avro Producer schema format to subscribe to your topic*

    ```python
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer( schema_registry_client,
                                      schema_str,
                                      msg_to_dict )

    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    producer.poll(0.0)
    ```
* ***Event Producer***
  * A Config file to paste your topic : [producer/config.py](producer/config.py)
  * An Event Producer to push to 5GMETA Platform : [producer/kafka-even-sender.py](producer/kafka-even-sender.py)

- Usage is:
  - Get the topic from the client in the utils [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client)
  - Modify [producer/config.py](producer/config.py) file and Paste your topic in the field of ***TOPIC***. See example below
    ```python
        TOPIC = 'KHALEDCHIKH90_1002_EVENT_109'
    ```
  - ``` python3 kafka-even-sender.py MESSAGE ```
