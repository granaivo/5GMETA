# Consumer examples
This folder contains Consumer examples for cits, video and image can be found in respective folders.

## Extra packages to be installed
First of all, you will need to install some dependencies (apt-get):

* python3-qpid-proton
* python3-avro
* python3-confluent-kafka
* gstreamer1.0-plugins-bad
* gstreamer1.0-libav
* python3-gst-1.0

Also install with pip3:

* kafka-python
* numpy
## Usage

The following examples must be used in combination of [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client) that returns apropriate topic and ip and ports to be run.
    
-   *Avro Consumer schema format to subscribe to your topic*
    ```python
        c = AvroConsumer({
        'bootstrap.servers': platformaddress+ ':' + bootstrap_port,
        'schema.registry.url':'http://'+platformaddress+':' + schema_registry_port, 
        'group.id': topic+'_'+generateRandomGroupId(4),
        'api.version.request': True,
        'auto.offset.reset': 'earliest'
    })

    c.subscribe([topic.upper()])
    ```

* ***Cits Consumer***
  * [cits/cits-consumer.py](cits/cits-consumer.py)
* ***Image Consumer***
  * [image/image-consumer.py](video/image-consumer.py)
* ***Video Consumer***
  * [video/video-consumer.py](video/video-consumer.py)

Usage is: 
- ```python3 cits-consumer.py topic platformaddress bootstrap_port registry_port```

- ```python3 video-consumer.py platformaddress bootstrap_port topic dataflow_id```

### More examples
There are other such examples that are complete and don't need to use external util to get topic and ip/port to access the system.

* [cits/cits-kafka-consumer.py](cits/cits-kafka-consumer.py)
* [image/image-kafka-consumer.py](image/image-kafka-consumer.py)
