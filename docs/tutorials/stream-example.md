# Consumer and Event Producer Examples
In this section, we walkthrough the Kafka consumer examples. Three sample consumer examples are provided within the [repo](https://github.com/5gmeta/stream-data-gateway/tree/main/examples/consumer) for cits, video and image datatypes respectively.

## Usage

### *Consumer instructions*
- Use [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client) to receive appropriate topics and IPs and ports to be used.

- Select the suitable consumer as per the produced data and use as follows: 
```
python3 cits-consumer.py topic platformaddress bootstrap_port registry_port

``` 
or

```
python3 video-consumer.py platformaddress bootstrap_port topic dataflow_id

```

# Avro events producer examples
This folder contains avro producer example to produce events.

## Usage
    
- Avro Producer config format to produce an event with avro serializer:
```python
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer( schema_registry_client, 
                                  schema_str,
                                  msg_to_dict )

producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                  'key.serializer': StringSerializer('utf_8'),
                  'value.serializer': avro_serializer}

producer = SerializingProducer(producer_conf)
.
.
.
producer.poll(0.0)
```

Usage is: 
```
python3 avro_producer_events.py -b bootstrap_ip:9092 -s http://schema_ip:8081 -t topic
```

# More examples
There are other such examples that are complete and don't need to use external util to get topic and ip/port to access the system.

* [cits/cits-kafka-consumer.py](https://github.com/5gmeta/stream-data-gateway/blob/main/examples/consumer/cits/cits-kafka-consumer.py)
* [image/image-kafka-consumer.py](https://github.com/5gmeta/stream-data-gateway/blob/main/examples/consumer/image/image-kafka-consumer.py)

