Consumer examples for cits, video and image can be found in respective folders.


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

The following examples must be used in combination of [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client) that returns apropriate topic and ip and ports to be run.

* [cits/cits-consumer.py](cits/cits-consumer.py)
* [image/image-consumer.py](video/image-consumer.py)
* [video/video-consumer.py](video/video-consumer.py)

Usage is: ```$ python3 cits-consumer.py topic platformaddress bootstrap_port registry_port```

Usage is: ```$ python3 video-consumer.py platformaddress bootstrap_port topic dataflow_id```

There are other such examples that are complete and don't need to use external util too get topic and ip/port to access the system.


* [cits/cits-kafka-consumer.py](cits/cits-kafka-consumer.py)
* [image/image-kafka-consumer.py](image/image-kafka-consumer.py)
