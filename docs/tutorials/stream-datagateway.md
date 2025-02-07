# Stream Data Gateway 

## Description
Stream Data Gateway is the module used in 5GMETA Cloud platform to stream the messages from the MECs to the Third Parties.
It is implemented using Kafka ecosystem technologies. This repository can be downloaded from [here](https://github.com/5gmeta/stream-data-gateway/tree/main).

## Table of Contents
The Stream Data Gatawey module contains:

- Kafka Broker docker-compose and helm chart ([link](https://github.com/5gmeta/stream-data-gateway/tree/main/src)). 

- Connectors configuration to retrive the messages from ActiveMQ (src/)

- **Examples** of different Kafka Consumers ([link](https://github.com/5gmeta/stream-data-gateway/tree/main/examples/consumer))


### Development version for Stream Data Gateway

- Refer this [README](https://github.com/5gmeta/stream-data-gateway/blob/main/src/README.md) for further API details. The dev version can be found [here](https://github.com/5gmeta/stream-data-gateway/tree/main/src/dev-version). There is Kafka instance to be deployed locally.

- Inside [***src/dev-version/connectors***](https://github.com/5gmeta/stream-data-gateway/tree/main/src/dev-version/connectors) there is CLI to create connectors between AMQP (MEC) and Kafka (Cloud) to push data into Kafka infrastructure.

### Production version

Contains all neccessary informations to deploy Kafka into an AWS infrastructure ([link](https://github.com/5gmeta/stream-data-gateway/tree/main/src/prod-version)).

### 5GMETA Platform client
This is a third party python client for making requests to 5GMETA Platform.
Clone the contents of [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client).
#### Prerequisites
  - Python3
  - Registered user in the platform. To register use: [5GMETA User Registration](5gmeta-platform.eu/identity/realms/5gmeta/account/)


### Examples
In the [***/examples***](https://github.com/5gmeta/stream-data-gateway/tree/main/examples) folder you can find different sample codes for different Kafka producers and consumers as follows:

#### Consumer:
- Kafka Consumers: working with AVRO (serialization and deserialization).
  - ***/consumer/cits/cits-consumer.py***: a sample python Kafka consumer to receive CITS messages.
  - ***/consumer/image/image-consumer.py***: a sample python Kafka consumer to receive images.
  - ***/consumer/video/video-consumer.py***: a sample python Kafka consumer to receive video streams.
  

#### Producer:
- A Kafka producer (***avro_producer_events.py***) as example for the Third Parties message producer.

#### Other:
- An interactive Avro CLI client to play with Kafka with AVRO ser-deser.
- Other example of producers
- Kafka Event message sender

### Installation
Installation requirements can be found on producer/consumer folders respectively.
- Clone the repository using ```git clone https://github.com/5gmeta/stream-data-gateway.git  ```

- Navigate to ***/examples*** folder to install dependencies using:
```
  pip install -r examples/requirements.txt
  
```

Kafka producer/consumer examples are described in further detail in the following section. 
