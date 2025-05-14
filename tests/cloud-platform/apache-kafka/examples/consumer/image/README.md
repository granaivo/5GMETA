# image-consumer
This code is an example of an image consumer that reads AVRO-encoded messages from a Kafka topic, decodes a base64 string, and saves the resulting binary image to disk.
## Dependencies
The following dependencies are required to run the code:

•	**confluent_kafka**: This is a Python client for Apache Kafka. It can be installed with **pip install confluent_kafka**.

•	**proton**: This is a Python binding for the Apache Qpid Proton library. It can be installed with pip install python-qpid-proton.

## Prerequisites
•	Access to a Kafka platform

•	Access to a schema registry

•	Access to a topic in the Kafka platform

## Usage

•	Install the dependencies using pip or any other package manager.

•	Run the **image-consumer.py** file with the following command:

**python3 image-consumer.py <topic> <platform_address> <bootstrap_port> <registry_port>**
  
•	Replace **<topic>, <platform_address>, <bootstrap_port>**, and **<registry_port>** with the actual values.
  
## Code Explanation
  
The **image-consumer.py** file is a Python script that implements an Avro Consumer to read messages from a Kafka topic. The Avro Consumer is created using the **confluent_kafka.avro.AvroConsumer** class.

The script takes four arguments from the command line: **topic, platform_address, bootstrap_port**, and **registry_port**. These arguments are used to set up the Avro Consumer with the correct configuration.

  The Avro Consumer subscribes to the specified topic using the **subscribe** method. The script then enters an infinite loop and starts polling for new messages using the poll method. When a new message is received, the script decodes the message and extracts the image data. The image data is then saved to a file.

  The script uses the **proton** library to decode the message body. The **base64** library is used to decode the image data.

  The script generates a random group id for the Avro Consumer using the **generateRandomGroupId** function. This is done to ensure that each instance of the script has a unique group id, which is required for Kafka's consumer group feature.

  The script prints debug information to the console, including the subscribed topics and any errors that occur during message consumption.
  
# image-kafka-consumer
  
  This script uses the Confluent Kafka Python library to consume messages from a Kafka topic. The consumed messages are expected to be in Avro format.
## Dependencies
The script requires the following dependencies to be installed:

•	**confluent-kafka**: A Python wrapper for the Confluent Kafka library

•	**requests**: A Python library for sending HTTP requests

•	**proton**: A Python library for messaging using AMQP

These can be installed using **pip**. For example:

**pip install confluent-kafka requests python-qpid-proton**
## Prerequisites
Before running the script, you need to have the following information:

•	platformaddress: the IP address of the Kafka broker

•	bootstrap_port: the port used to bootstrap the Kafka connection

•	registry_port: the port used to communicate with the Kafka schema registry

•	platformuser: the username to authenticate with the Kafka broker

•	platformpassword: the password to authenticate with the Kafka broker
# Usage
To run the script, simply execute it using Python:

**python kafka_consumer.py**
## Code Explanation
The script first imports the required libraries, including **confluent_kafka, requests,** and **proton**.

It then sets the values for the **platformaddress, bootstrap_port, registry_port, platformuser**, and **platformpassword** variables.

Next, the script sends an HTTP request to the Kafka broker to get the topic to consume from. The topic is stored in the **topic** variable.

The script then creates an **AvroConsumer** object using the **confluent_kafka** library, specifying the **bootstrap.servers** and **schema.registry.url** properties, as well as the consumer group ID and other properties.

The script then subscribes to the Kafka topic using the **subscribe** method of the **AvroConsumer** object.

The script then enters an infinite loop, where it waits for messages using the **poll** method of the **AvroConsumer** object. If a message is received, the script decodes it from Avro format and extracts the message body. It then writes the message body to a file.

The script continues to wait for messages until it is interrupted by the user. When the user interrupts the script, it closes the **AvroConsumer** object using the **close** method.

# security
  
  ## Dependencies
This code requires the requests library, which can be installed via pip:

**pip install requests** 
## Prerequisites
To use this code, you will need to have a valid username and password for the 5gmeta-platform.eu authentication server. You will also need to know the client_id for the application you are trying to authenticate with.
## Usage
1.	Install requests library if it is not already installed.
2.	Import the requests library and the code above into your project.
3.	Call get_auth_token(user, password) to get an access token.
4.	Call get_header_with_token(user, password) to get a header with the access token.
## Code Explanation
The code defines two functions, **get_auth_token(user, password)** and **get_header_with_token(user, password)**.

**get_auth_token(user, password)** function sends a POST request to an authentication endpoint with the given user and password credentials. The response of the request is a JSON object that includes an access token. The function returns the access token.

**get_header_with_token(user, password)** function calls get_auth_token(user, password) to get the access token. It then formats the access token in a header and returns the header.



