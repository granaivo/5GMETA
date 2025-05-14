# video-consumer
This is a Python program that reads video frames from a Kafka topic and displays them on screen using GStreamer.
## Prerequisites
Before running this program, you need to have the following installed:

•	Python 3.x

•	GStreamer 1.x

•	Kafka-python library

•	avro-python3 library

•	Apache Qpid Proton library

You can install these using pip as follows:

**pip install kafka-python avro-python3 python-qpid-proton**

## Usage
To run this program, use the following command:

**python video_reader.py <platform_ip> <kafka_port> <topic> <sourceId>** 

  where:
  
•	platform_ip: the IP address of the Kafka broker.
  
•	kafka_port: the port number on which Kafka is listening.
  
•	topic: the name of the Kafka topic to consume from.
  
•	sourceId: the ID of the video stream to display.
  
## Code Explanation
### Importing Libraries
The required libraries are imported at the beginning of the program.
### Initializing Variables
The program initializes variables to store various parameters like the pipeline, AppSrc, presentation timestamp (pts), frame duration, etc.
### Initializing GStreamer
GStreamer is initialized using the **Gst.init** method.
### Building the Pipeline
The GStreamer pipeline is built using the **Gst.parse_launch** method.
## Setting Properties of AppSrc
The AppSrc is obtained from the pipeline using **pipeline.get_by_name("appsrc")** and various properties of **AppSrc** are set using the **set_property** method.
### Playing the Pipeline
The pipeline is started using **pipeline.set_state(Gst.State.PLAYING)**.
### Reading from Kafka Topic
The program subscribes to a Kafka topic using the **KafkaConsumer** from the **kafka-python** library. It then waits for messages to be received from the topic.
### Decoding the Message
The received message is decoded using the **decode** function, which takes a message value and returns a dictionary containing the properties and payload of the message.
### Extracting Video Payload
The payload of the message is obtained and parsed using the **proton** library.
### Displaying the Video
The video is displayed on screen by pushing the video buffer to the **AppSrc** using **appsrc.emit("push-buffer", gst_buffer)**. The **push-buffer** signal is emitted with the **Gst.Buffer** as a parameter.
### Freeing Resources
The pipeline is stopped and resources are freed using **pipeline.set_state(Gst.State.NULL)**.
  
  #video-schema
  
  ## Code Explanation
The jms record is an Avro schema that defines the structure of a JMS message. It contains fields that correspond to standard JMS message properties, such as message timestamp, correlation ID, and priority.
 The schema defines the following fields:
•	message_timestamp: The timestamp of the JMS message.

•	correlation_id: The correlation ID of the JMS message.

•	redelivered: Whether the JMS message has been redelivered.

•	reply_to: The destination where a reply to the JMS message should be sent.

•	destination: The destination where the JMS message was sent.

•	message_id: The ID of the JMS message.

•	mode: The delivery mode of the JMS message.

•	type: The type of the JMS message.

•	priority: The priority of the JMS message.

•	bytes_payload: The payload of the JMS message as a byte array.

•	properties: The properties of the JMS message as a map of key-value pairs.



