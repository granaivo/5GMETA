# 5GMETA project
# Client example write by D.Amendola - Vicomtech 
# This client use the KSQLdb python client wrapper for the KSQLdb APIs from here: https://github.com/bryanyang0528/ksql-python
# 
# How to install: pip install ksql

import time
import logging
from urllib.parse import scheme_chars
from ksql import KSQLAPI
from matplotlib import use
from numpy import source
logging.basicConfig(level=logging.DEBUG)

platformaddress = "<ip>" 
ksqlapi_port = "8088" 
mecaddress = "ip" 

# platformaddress = "localhost"
# mecaddress = "localhost"

client = KSQLAPI("http://"+platformaddress+":"+ksqlapi_port)

# with basic authentication
# client = KSQLAPI('http://ksql-server:8088', api_key="your_key", secret="your_secret")

def streams_list():
    arr_res = client.ksql('show streams')
    streams = next(item for item in arr_res if item['streams']!="")['streams']
    return list(map(lambda stream: stream['name'], streams))

def connectors_running_list():
    arr_res = client.ksql('show connectors')
    connectors = next(item for item in arr_res if item['connectors']!="")['connectors']
    return list(map(lambda item: item['name'], filter( lambda item: item['state'].find('RUNNING')!=-1 ,connectors)))

def stream_exists(stream_name):
    if(stream_name.upper() in streams_list()):    
        return True
    return False

def drop_stream(stream_name):
    client.ksql('drop stream ' + stream_name)

def create_stream_from_topic(stream_name, topic_name):
    if(not stream_exists(stream_name)):
        create_stream = "CREATE STREAM " + stream_name + " \n" \
                        "WITH (kafka_topic='" + topic_name + "', value_format='AVRO');"
        client.ksql( create_stream )
        return True
    return False

# SINK: from MEC (ActiveMQ) to Kafka
def create_datatype_connector( datatype, name="JMS_SOURCE", activemq_url = "tcp://<ip>:61616" ):
    # Create a connector from MEC to Cloud.
    # 
    # Create e connector to gather the messages from ActiveMQ to a Kafka tipic.
    # the datatype will be used as name for the topic both in ActiveMQ and in Kafka.
    # author d.amendola
    username_activemq = "<user>"
    password_activemq = "<password>"
    schema_registry_url = 'http://'+platformaddress+':8081' #'http://schema-registry:8081'

    create_connector = "CREATE SOURCE CONNECTOR `" + name + "` WITH (\n" \
                "'connector.class'= 'com.datamountaineer.streamreactor.connect.jms.source.JMSSourceConnector',\n" \
                "'connect.jms.initial.context.factory'= 'org.apache.activemq.jndi.ActiveMQInitialContextFactory',\n" \
                "'tasks.max'= '1',\n" \
                "'connect.jms.queues'= 'jms-queue',\n" \
                "'connect.jms.password'= '" + password_activemq + "',\n" \
                "'connect.progress.enabled'= 'true',\n" \
                "'connect.jms.username'= '" + username_activemq + "', \n" \
                "'connect.jms.url'= '" + activemq_url + "',\n" \
                "'connect.jms.kcql'= 'INSERT INTO " + datatype + " SELECT * FROM " + datatype + " WITHTYPE TOPIC ',\n" \
                "'connect.jms.connection.factory'= 'ConnectionFactory',\n" \
                "'name'= '" + name + "'\n" \
                ");" 
                #"'value.converter.schema.registry.url'= '" + schema_registry_url + "' ,\n" \
                #"'value.converter.enhanced.avro.schema.support'= 'true',\n" \
                #"'value.converter'= 'io.confluent.connect.avro.AvroConverter',\n" \
                #"'key.converter'= 'org.apache.kafka.connect.storage.StringConverter'\n" \
    client.ksql( create_connector )   

# SINK: from Kafka to MEC (ActiveMQ)
def create_sink_messages_connector( source_topic="event", dest_topic="event", name="JMS_SINK", activemq_url = "tcp://192.168.15.181:61616" ):
    # Create a connector from Cloud to the MEC.
    # 
    # Create e connector to gather the messages from Cloud platform to a destination MEC.
    # the source and destination topics will be used for creating topic both in ActiveMQ from the topic in Kafka.
    # author d.amendola
    username_activemq = "<user>"
    password_activemq = "<password>"
    schema_registry_url = 'http://'+platformaddress+':8081' #'http://schema-registry:8081'

    create_connector = "CREATE SINK CONNECTOR `" + name + "` WITH (\n" \
                "'connector.class'= 'com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector',\n" \
                "'connect.jms.initial.context.factory'= 'org.apache.activemq.jndi.ActiveMQInitialContextFactory',\n" \
                "'tasks.max'= '1',\n" \
                "'topics'= '"+source_topic+"',\n" \
                "'connect.jms.password'= '" + password_activemq + "',\n" \
                "'connect.progress.enabled'= 'true',\n" \
                "'connect.jms.username'= '" + username_activemq + "', \n" \
                "'connect.jms.url'= '" + activemq_url + "',\n" \
                "'connect.jms.kcql'= 'INSERT INTO " + dest_topic + " SELECT * FROM " + source_topic + " WITHTYPE TOPIC WITHFORMAT JSON',\n" \
                "'connect.jms.connection.factory'= 'ConnectionFactory',\n" \
                "'name'= '" + name + "'\n" \
                ");"
                 # "'connect.jms.queues'= 'jms-queue',\n" \
                #"'value.converter.schema.registry.url'= '" + schema_registry_url + "' ,\n" \
                #"'value.converter.enhanced.avro.schema.support'= 'true',\n" \
                #"'value.converter'= 'io.confluent.connect.avro.AvroConverter',\n" \
                #"'key.converter'= 'org.apache.kafka.connect.storage.StringConverter'\n" \
    client.ksql( create_connector )   


def query_stream_to_topic( datatype_stream, dest_stream="topic_filtered", sender_id="", tile_filter="" ):
    # Create a topic with data filter from a datatype.
    # Example of use query_stream_to_topic( 'CITS_STREAM', 'CITS_VEHICLE_FILTERED' ) -> no contraints
    #                query_stream_to_topic( 'CITS_STREAM', 'CITS_VEHICLE_FILTERED', source_id = "v1", tile_filter="1230%")
    # 
    # IMPORTANT: The dest_stream name will be also the name of the destination topic!

    if(not stream_exists(datatype_stream)):
        print("ERROR: Source stream not exists - " + datatype_stream)
        return False

    if(stream_exists(dest_stream)):
        drop_stream(dest_stream)
    
    statement_filter = "CREATE STREAM " + dest_stream + " AS SELECT * \n" \
                "FROM " + datatype_stream + " " 
    if sender_id=="" and tile_filter=="":
        # no filter on the datatype
        statement_filter += ";"
    else:
        statement_filter += "WHERE \n"
        if sender_id!="":
            statement_filter += "`PROPERTIES`['sender_id']='" + sender_id + "' \n"     
        if tile_filter!="":
            statement_filter += "AND `PROPERTIES`['tile'] LIKE '" + tile_filter + "';" 
    # Execute the statement as requested
    client.ksql( statement_filter )



# Main for testing purpose
# 
# author: d.amendola
def main():

    #print(client.ksql('show streams'))
    
    # TODO: to be fixed with check connector 
    #if(not stream_exists('cits_stream')):
        
    create_datatype_connector("cits", "JMS_SourceCITS", "tcp://"+mecaddress+":61616")

    #create_sink_messages_connector("events_5gmeta", "dest_tp_event", "JMS_SINK_TP1", "tcp://<ip>:61616")

    prompt_var = ""
    while(prompt_var!="y"):
        print(streams_list())
        print("Before querying check that the topic and the associated schema into the registry exist!")
        prompt_var = input("Continue... ? [y/n]")

    # ATTENTION: when create a stream check that the topic and the associated schema into the registry exist!
    create_stream_from_topic("cits_stream", "cits")
    # Then query a topic to filter it
    query_stream_to_topic('cits_stream', 'vicomtech_cits_query001', "v-1")   # 0123012301230123001  
    print("Streams list: ")
    print(streams_list())
# 
if __name__ == "__main__":
    main()
