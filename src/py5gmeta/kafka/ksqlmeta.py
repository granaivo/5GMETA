import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def list_streams(client):
    arr_res = client.ksql('show streams')
    streams = next(item for item in arr_res if item['streams']!="")['streams']
    streams_list = list(map(lambda stream: stream['name'], streams))
    logger.debug(streams_list)
    return streams_list

def list_running_connectors(client):
    arr_res = client.ksql('show connectors')
    connectors = next(item for item in arr_res if item['connectors']!="")['connectors']
    return list(map(lambda item: item['name'], filter( lambda item: item['state'].find('RUNNING')!=-1 ,connectors)))

def exists_stream(client, stream_name):
    return stream_name.upper()  in list_streams(client)

def drop_stream(client, stream_name):
    client.ksql('drop stream ' + stream_name)

def create_stream_from_topic(client, stream_name, topic_name):
    if not exists_stream(client, stream_name):
        create_stream = "CREATE STREAM " + stream_name + " \n" \
                        "WITH (kafka_topic='" + topic_name + "', value_format='AVRO');"
        client.ksql( create_stream )
        return True
    return False

def get_connector(name, username_activemq, password_activemq, activemq_url, datatype):

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
    logger.debug(create_connector)
    return create_connector

def create_datatype_connector(client, datatype, name, activemq_url, username, password):
    client.ksql( get_connector(name, username, password, activemq_url, datatype) )

# SINK: from Kafka to MEC (ActiveMQ)
def create_sink_messages_connector( client, source_topic, dest_topic, name, activemq_url, amqp_user, amqp_password, schema_reqistry_url ):
    """
    :param client:
    :param source_topic:
    :param dest_topic:
    :param name:
    :param activemq_url:
    :param amqp_user:
    :param amqp_password:
    :param schema_reqistry_url:
    :return:
    """
    create_connector = "CREATE SINK CONNECTOR `" + name + "` WITH (\n" \
                "'connector.class'= 'com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector',\n" \
                "'connect.jms.initial.context.factory'= 'org.apache.activemq.jndi.ActiveMQInitialContextFactory',\n" \
                "'tasks.max'= '1',\n" \
                "'topics'= '"+source_topic+"',\n" \
                "'connect.jms.password'= '" + amqp_password + "',\n" \
                "'connect.progress.enabled'= 'true',\n" \
                "'connect.jms.username'= '" + amqp_user + "', \n" \
                "'connect.jms.url'= '" + activemq_url + "',\n" \
                "'connect.jms.kcql'= 'INSERT INTO " + dest_topic + " SELECT * FROM " + source_topic + " WITHTYPE TOPIC WITHFORMAT JSON',\n" \
                "'connect.jms.connection.factory'= 'ConnectionFactory',\n" \
                "'name'= '" + name + "'\n" \
                ");"

    client.ksql(create_connector)


def query_stream_to_topic( client, datatype_stream, dest_stream="topic_filtered", sender_id="", tile_filter="" ):
    """
    Create a topic with data filter from a datatype.
    Example of use query_stream_to_topic( 'CITS_STREAM', 'CITS_VEHICLE_FILTERED' ) -> no contraints
                    query_stream_to_topic( 'CITS_STREAM', 'CITS_VEHICLE_FILTERED', source_id = "v1", tile_filter="1230%")

    IMPORTANT: The dest_stream name will be also the name of the destination topic!
    :param client:
    :param datatype_stream:
    :param dest_stream:
    :param sender_id:
    :param tile_filter:
    :return:
    """
    if not exists_stream(datatype_stream):
        print("ERROR: Source stream not exists - " + datatype_stream)
        return False
    else:
        drop_stream(client,  dest_stream)
    
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
    client.ksql(statement_filter)
