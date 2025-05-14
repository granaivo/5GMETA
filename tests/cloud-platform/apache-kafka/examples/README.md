# List of client examples

In the folder Examples you can find the following folders.

Consumer:
- A Kafka Consumer: working with AVRO (serialization and deserialization).

Producer:
- A Kafka producer (avro_producer_events.py) as example for the ThirtParties message producer.

Admin:
- An example of Kafka Admin

Other:
- Event message sender

## Kafka CITS consumer
For 5GMETA an example of CITS consumer is in the file: [cits/cits-consumer.py](cits/cits-consumer.py)
This example will ask for a Kafka topic, a kafka ip, bootstrap port and a registry port

## Kafka image consumer
For 5GMETA an example of image consumer is in the file: [image/cits-consumer.py](image/image-consumer.py)
This example will ask for a Kafka topic, a kafka ip, bootstrap port and a registry port

## Kafka event sender
An example of producer for the project 5GMETA is ready within the file: [producer/kafka_event_sender.py](producer/kafka_event_sender.py)

## KSQLdb administration client
The KSQLdb client is writen in python. The client provides the functionality to move the messager from a MEC to the Cloud platform of 5GMETA, moreover provide the functionality to filter and manage the message, create destination topic to be exposed to the third parties.

An example of main is provided: 
 - (i) creating a connector to import the messages from an ActiveMQ message broker into a given topic of Kafka;
 - (ii) creating a query to filter the messages by `source_id` or by `tile` (geo-filter);
 - (iii) support methods are provided.

on examples/ksql
 ```
 $ python3 client-ksql.py
 ```

 ## Ksqldb debug tool

 There is way to connect into ksqldb command line interface to do some manual queries into ksqldb.
 * src/dev-version
 * make ksql-cli
 You will be prompted into the ksqldb cli.

 Some examples:
* show queries;
* terminate query_id;
* show topics;
* show streams;
* ksqldb.io/examples.html

