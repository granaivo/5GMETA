# Testing the 5GMETA Platform

## Introduction

This document describes some Python scrips to test the 5GMETA Platform. The test are in the folder [./tests](./tests).

## Pre-requisities

- Create a Python virtuel env
- Install the requirements [python-qpid-proton](https://pypi.python.org/pypi/python-qpid-proton) - including any of its [dependencies](https://github.com/apache/qpid-proton/blob/master/INSTALL.md)
- Linux distribution
- Python version 3.5+
- (you have to modify **address.py** in order to put the ***appropriate ip***, ***port*** and ***topic*** given by **your message broker**)


## Sample Datasets to test a CAM application

The table lists the datasets which can be used during the development of a CAM application.


| 5GMETA DATA TYPE | DESCRIPTION                                                                                                         | TILE               | SAMPLE                                                                                                                                                                                             | ReadMe |
|------------------|---------------------------------------------------------------------------------------------------------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| C-ITS Simulation | Cooperative Awareness Messages (position, heading, speed, acceleration, etc...) from simulated vehicles             | anywhere/worldwide | [WebApp UI](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/image-sample-links-simulator-ui.jpg)                                                                                       | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/unimore-cam-description.md) |
| C-ITS            | Position, heading, speed and acceleration from vehicles  in Donostia (Spain)                                        | 0313331232         | [cits-vicomtech-donostia.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-vicomtech-donostia.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/vicomtech_cam_description.md) |
| C-ITS            | Position, heading, speed and acceleration from vehicles  in Toulouse (France)                                       | 120222021          | [cits-vicomtech-toulouse.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-vicomtech-toulouse.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/vicomtech_cam_description.md) |
| C-ITS            | Category, Position, orientation, speed and cam_id, object_id from vehicles, person, bycicle..etc  in MODENA (Italy) | 1202231113220102   | [cits-unimore-modena-masa.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-unimore-modena-masa.json)             | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/unimore-masa-description.md) |
| C-ITS            | Camera_id, space_id ,space position, Occupied Or Empty, from Parking Lots in MODENA (Italy)                         | 1202231113220102   | [cits-unimore-modena-pld.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-unimore-modena-pld.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/unimore-pld-description.md) |
| C-ITS            | GPS Position, dms_level, dms_trigger from Driver monitoring system in MODENA (Italy)                                | 1202231113220102   | [cits-unimore-modena-dms.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-unimore-modena-dms.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/unimore-dms-description.md) |
| C-ITS            | Position, heading, speed and acceleration from vehicles in MODENA (Italy)                                           | 1202231113220102   | [cits-unimore-modena-cam.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-unimore-modena-cam.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/unimore-cam-description.md) |
| image            | jpg from   Donostia (Spain)                                                                                         | 0313331232         | [image-sample-vicomtech-donostia.jpg](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/image-sample-vicomtech-donostia.jpg) | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/vicomtech_jpg_description.md)|
| image            | jpg from   Toulouse (France)                                                                                        | 120222021          | [image-sample-vicomtech-toulouse.jpg](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/image-sample-vicomtech-toulouse.jpg) | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/vicomtech_jpg_description.md)|
| C-ITS            | Position, heading, speed and acceleration from vehicles  in Versailles (France)                                     | 1202200101311      | [cits-versailles_area-cam.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-versailles_area-cam.json)             | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/versailles_cam_description.md)|
| C-ITS            | Status of traffic light in Versailles (France)                                                                      | 1202200101311      | [cits-versailles_area-spat.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-versailles_area-spat.json)           | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/versailles_spat_description.md)|
| C-ITS            | Parking area occupancy  in Versailles (France)                                                                      | 1202200101311      | [cits-versailles_area-pam.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/cits-versailles_area-pam.json)             | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/versailles_pam_description.md)|
| C-ITS            | Traffic counting from with 2 metric (Throughput, Occupation rate) with 5 sensors in Versailles (France)             | 1202200101311      | [traffic_sensors_samples.json](https://github.com/5gmeta/5gmeta-dev/blob/main/datasets/traffic_sensors_samples.json)               | [README](https://github.com/5gmeta/5gmeta-dev/blob/main/docs/datasets-description/versailles_traffic_sensors_description.md)|



## Image Sender

The examples can be found in the folder [./tests/examples/message-data-broker/image_sender_python](./tests/examples/message-data-broker/image_sender_python).

Similarly to the [C-ITS sender example](cits-sender-message-data-broker.md), you need to configure the address of the data broker.

Then, in one terminal window run:

```bash
python3 sender.py
```

For S&D connected to a database, the images to be  send are stored in sample_images folder. The messages being sent by the sender can be consummed by typing:

```bash
python3 receiver.py
```

### Debegging using ActiveMQ Web Console

The ActiveMQ admin Web Console can be used to check Messages Enqueued / Dequeued counts match.

You can control which AMQP server the examples try to connect to and the messages they send by changing the values in config.py

You have to take into account that any modification made on dataflowmetadata must be applied too into the content.py file in order to generate the appropriate content.


## C-ITS Data  Sender

The examples can be found in the folder [./tests/examples/message-data-broker/cits_sender_python](./tests/examples/message-data-broker/cits_sender_python).

- Modify [**address.py**](https://github.com/5gmeta/message-data-broker/blob/main/examples/activemq_clients/cits_sender_python/address.py) to put the ***appropriate ip***, ***port*** and ***topic*** given by **your message broker** or run with options:

- Additional arguments as highlighted below could be parsed to the [sender.py](https://github.com/5gmeta/message-data-broker/blob/main/examples/activemq_clients/cits_sender_python/sender.py) :

```bash
- h, --help            show this help message and exit
- a ADDRESS, --address=ADDRESS
= address to which messages are sent (default )
- m MESSAGES, --messages=MESSAGES
= number of messages to send (default 100)
- t TIMEINTERVAL, --timeinterval=TIMEINTERVAL
= messages are sent continuosly every time interval seconds (0: send once) (default 10)
```

- In one terminal window run wither of the sender scripts **depending upon whether you are running your S&D connected to an database or not**. You can add additional arguments as shown before:

```bash
python3 sender.py
```

Or test by:

```bash
python3 sender_with_sd_database_support.py
```

Example output can be seen below:

![Sender example](docs/images/sender_data_broker.png)


To debug and check if messages are being sent,  run in another terminal to receive messages:

Run to see the received messages on the subscribed AMQP topic.

```bash
python3 receiver.py
```

![Receiver example](docs/images/receiver_data_broker.png)

- Use the ActiveMQ admin web page to check Messages Enqueued / Dequeued counts match.

- You can control which AMQP server the examples try to connect to and the messages they send by changing the values in **config.py**

- ***NB:*** You have to take into account that any modification made on dataflowmetadata must be applied too into the content.py file in order to generate the appropriate content.

### Pseudo movement example

This example demonstrates data being produced by a moving sensor device.

- Added some movement around a fixed GPS position in order to simulate movement. Example:
       - [cits_send_moving_location.py]

This way we can move around a MEC that covers tiles:

* 031333123201033
* 031333123201211

and a ***secondary one*** that covers tiles:

* 031333123201212
* 031333123201213
* 031333123201223
* 031333123202223


## End-to-End Data Production and Consuption Examples

### Description

As introduced in the previous section, provides examples how to produce (cits/image) data from a Sensor&Device to the MEC using Python AMQP reactor API with ActiveMQ.

In this section, we will explain in further detail how to implement a data producer. Specific examples will then be provided in the following pages of this documentation.

Refering the examples presented, following are some essential parameters while producing data on the 5GMETA platform:

- ***source_id***: a Unique Identifier to distinguish the source of generated data.
- ***tile***: Tile of the source from where the data is being generated in form of QuadKey code. e.g. 1230123012301230 (must be 18 chars in [0-3])
- ***datatype***: should be one of the allowed datatype [cits, video, image]
- ***sub_datatype***: depends upon on the datatype e.g. cam, denm, mappem

### Generic producer structure

A typical producer will contain the following fields, as it can be seen in the [examples](https://github.com/5gmeta/message-data-broker/blob/main/examples/activemq_clients):

- ***Discovery Registration API*** : This API helps you connect your S&Ds and push data to the MEC within a specified tile.


- Getting tile of the source from its current GPS position:

```python
tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
```


- Getting the **message-broker access** from the **MEC** within the **previous tile**:

```python
service="message-broker"

messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
```

- Getting AMQP **Topic** and **dataFlowId** to **push** data into the **Message Broker**:

```python
dataflowId, topic = discovery_registration.register(dataflowmetadata,tile)

opts.address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+":/topic://"+topic

jargs = json.dumps(args)
```

#### Usage

Let's take an example of CITS message producer as shown here in [sender.py](https://github.com/5gmeta/message-data-broker/blob/main/examples/activemq_clients/cits_sender_python/sender.py) for reference.

- Pass the latitude and longitude GPS position of your sensor device as shown here in :

```
# Geoposition - Next steps: from GPS device.
latitude = 43.3128
longitude = -1.9750

```
- Replace with your ***metadata*** in this section shown below.

```json
dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "cits",
        "dataSubType": "json"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
        "extraAttributes": None,
    },
    "licenseInfo": {
        "licenseGeolimit": "europe",
        "licenseType": "profit"
    },
    "dataSourceInfo": {
        "sourceTimezone": 2,
        "sourceStratumLevel": 3,
        "sourceId": 1,
        "sourceType": "vehicle",
        "sourceLocationInfo": {
            "locationQuadkey": tile,
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }
}

```

- Use the sample [content.py](https://github.com/5gmeta/message-data-broker/blob/main/examples/activemq_clients/cits_sender_python/content.py) to generate your messages. Here as you can see the ***msgbody** contains the CITS message.

```python
def messages_generator(num, tile, msgbody='body_cits_message'):
    messages.clear()

    #print("Sender prepare the messages... ")
    for i in range(num):
        props = {
                    "dataType": "cits",
                    "dataSubType": "cam",
                    "dataFormat":"asn1_jer",
                    "sourceId": 1,
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msgbody))
                    }

        messages.append( Message(body=msgbody, properties=props) )
```

## Credits

TODO: Add the credits

## Conclusion

TODO: Add the conclusion

## References

- https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/python/docs/tutorial.html
- https://access.redhat.com/documentation/en-us/red_hat_amq/6.3/html/client_connectivity_guide/amqppython
