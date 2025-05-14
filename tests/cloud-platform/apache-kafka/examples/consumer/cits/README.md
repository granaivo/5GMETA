# cits-consumer.py
## Description

This Python script is a Kafka consumer that retrieves messages from a Kafka topic and processes them. In particular, it deserializes AVRO messages and decodes QPID Proton messages from their binary representation.
## Prerequisites

This script requires Python 3 and the following dependencies:

•	confluent-kafka

•	confluent-kafka[avro]

•	requests

•	proton

These dependencies can be installed using pip:

pip install confluent-kafka confluent-kafka[avro] requests proton 
## Usage
To run the script, use the following command:
python3 cits-consumer.py topic platformaddress bootstrap_port schema_registry_port 
where:

•	topic is the name of the Kafka topic to consume messages from

•	platformaddress is the IP address or hostname of the Kafka broker

•	bootstrap_port is the port number of the Kafka broker

•	schema_registry_port is the port number of the Confluent Schema Registry

For example, to consume messages from the topic "mytopic" on a Kafka broker running at IP address 192.168.1.100 on port 9092 and Confluent Schema Registry running on port 8081, use the following command:
python3 cits-consumer.py mytopic 192.168.1.100 9092 8081 
## Code Explanation
1.	The script imports the required dependencies including Consumer, KafkaException, json, logging, pformat, KafkaError, AvroConsumer, SerializerError, TopicPartition, base64, and requests.
2.	The generateRandomGroupId function is defined to generate a random group ID for the consumer to use.
3.	The script checks the number of command-line arguments and exits if it is not equal to 5.
4.	The script retrieves the command-line arguments and sets them to variables.
5.	An AvroConsumer instance is created with the Kafka broker and Schema Registry configuration. The consumer subscribes to the specified topic.
6.	The script enters a loop to poll for new messages.
7.	If no message is received, the loop continues.
8.	If a message is received, it is checked for errors. If an error occurs, it is logged and the loop continues.
9.	If no error occurs, the message is logged to the console.
10.	The mydata variable retrieves the AVRO message from the message payload.
11.	The raw_sd variable retrieves the binary representation of the QPID Proton message from the BYTES_PAYLOAD field of the AVRO message.
12.	The msg_sd variable is an instance of proton.Message and is used to decode the QPID Proton message from its binary representation.
13.	The msg_sd.body field contains the data of the sender.
14.	The data is written to a file with a name based on the current iteration of the loop.
15.	The loop continues until the consumer is closed with c.close().

# cits-kafka-consumer

## Code Explanation
The Kafka Avro Consumer script subscribes to a Kafka topic and consumes messages from it. The script uses the confluent_kafka library to create an AvroConsumer instance, which is used to subscribe to the specified Kafka topic.

When a message is received, the script decodes the Avro message and extracts the BYTES_PAYLOAD field. The BYTES_PAYLOAD field contains a QPID proton message that is then decoded using the proton library. The body of the decoded message is then written to a file.

The script also handles errors that may occur when consuming messages from the Kafka topic. If an error occurs, the script will log the error message and continue consuming messages.

## Dependencies

•	confluent_kafka (version 1.7.0 or higher)

•	requests (version 2.26.0 or higher)

•	proton (version 0.35.0 or higher)
## Prerequisites
Before running the Kafka Avro Consumer, you will need to have the following:

•	Access to a Kafka cluster

•	The Kafka topic that you want to consume messages from

•	The schema registry URL for the Kafka cluster

•	The group ID for the consumer

In addition, you will need to have the following information:

•	The address of the Kafka bootstrap server

•	The port number for the Kafka bootstrap server

•	The port number for the schema registry

•	The credentials for the Kafka platform

## Usage
1.	Install the dependencies by running pip install confluent_kafka requests proton.
2.	Set the values for the following variables: tile, platformaddress, bootstrap_port, registry_port, platformuser, and platformpassword.
3.	Replace get_header_with_token with your own function that returns a header with the appropriate token for the Kafka platform.
4.	Run the script.

# security

## Code Explanation
The **get_auth_token** function takes a user and password as arguments, and uses them to make a POST request to the 5gmeta-platform.eu website to retrieve an access token. The access token is returned as a string.
The **get_header_with_token** function takes a user and password as arguments, and uses the **get_auth_token** function to retrieve an access token. The access token is then added to the **Authorization** header, and the header is returned as a dictionary.
The **headers** and **data** variables are used to build the request headers and data. The **headers** variable specifies the **Content-Type** of the request, while the **data** variable includes the **grant_type, username, password,** and **client_id** required for the request.
The **requests.post** function is used to make the POST request to the website's token endpoint. The response is then parsed as a JSON object, and the **access_token** field is returned from the object. Finally, the **Bearer** prefix is added to the access token and returned as part of the header dictionary.
## Dependencies
This code **requires** the requests library, which can be installed via pip.
pip install requests 
## Prerequisites
This code is designed to retrieve an authentication token for a given user and password from the 5gmeta-platform.eu website. To use this code, you will need valid login credentials for that website.
## Usage
1.	The code imports the 'requests' library which is used for making HTTP requests in Python.
2.	The code defines a function named 'get_auth_token' which takes two parameters 'user' and 'password'.
3.	Inside the 'get_auth_token' function, it creates a dictionary named 'headers' containing the 'Content-Type' key and value.
4.	It creates another dictionary named 'data' containing the 'grant_type', 'username', 'password', and 'client_id' keys and their corresponding values.
5.	It sends a POST request to the 'https://5gmeta-platform.eu/identity/realms/5gmeta/protocol/openid-connect/token' endpoint using the 'requests.post' method, passing the 'headers' and 'data' dictionaries as arguments.
6.	The response received from the server is converted into a JSON format using the 'response.json()' method and stored in the variable 'r'.
7.	The function returns the 'access_token' value from the JSON response.
8.	The code defines another function named 'get_header_with_token' which takes the same parameters as 'get_auth_token'.
9.	It calls the 'get_auth_token' function with the 'user' and 'password' parameters to obtain the 'access_token' value.
10.	It concatenates the 'Bearer' keyword with the 'access_token' value and stores it in the variable 'token'.
11.	It creates a dictionary named 'headers' containing the 'Authorization' key and the 'token' variable as its value.
12.	The function returns the 'headers' dictionary.
