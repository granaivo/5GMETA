Kafka Topic Creation Script

Introduction

This code is using the Confluent Kafka Python library to create two new Kafka topics named "topic1" and "topic2". The new topics are created with three partitions and a replication factor of 1.
The code starts by creating an instance of the AdminClient class using the bootstrap server at localhost:9092.
Then it creates a list of NewTopic objects, one for each topic to be created. The num_partitions argument specifies the number of partitions for each topic, while the replication_factor argument specifies the number of replicas for each partition. In this case, the replication factor is set to 1, which means that each partition will have only one replica.
Next, the create_topics method is called on the AdminClient object, passing in the list of new topics. This method asynchronously creates the new topics and returns a dictionary of futures, where each future corresponds to a topic.
Finally, the code waits for each future to complete by iterating through the dictionary of futures and calling the result method on each one. If the future completed successfully, the code prints a message indicating that the topic was created. If the future raised an exception, the code prints an error message indicating that the topic creation failed.

Dependencies

•	confluent-kafka 
The dependencies can be installed using the following command:
pip install confluent-kafka

Prerequisites

•	A running Kafka cluster accessible via bootstrap.servers parameter in the script.
•	Python 3.x installed on the system.
•	Confluent Kafka Python library installed (confluent-kafka package).

Usage
1.	Install the required dependencies using pip:
               pip install confluent-kafka 
2.	Modify the script to define the desired topic names, number of partitions, and replication factor.
new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["topic1", "topic2"]]
3.	Run the script using Python:
python kafka_topic_creation.py

Notes

•	By default, the replication factor is set to 1 in this script. In a production scenario, it is more typical to use a replication factor of 3 for durability.

•	The script uses the create_topics method of the AdminClient class to asynchronously create topics. It returns a dictionary of futures where each future corresponds to a topic. The code waits for each future to complete using the result method.

•	If the future completed successfully, the script prints a message indicating that the topic was created. If the future raised an exception, the script prints an error message indicating that the topic creation failed.

