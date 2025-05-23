import unittest
from ksql import KSQLAPI
from py5gmeta.kafka import ksqlmeta
from collections.abc import Iterable

class KSQLMETATestCase(unittest.TestCase):
    def setUp(self):
        self.client = KSQLAPI("https://cloudplatform.francecentral.cloudapp.azure.com/ksql/")
        self.data_type = "cits"
        self.name = "JMS_SourceCITS"
        self.activemq_url = "tcp://akkodismec.francecentral.cloudapp.azure.com:61616"
        self.username = "5gmeta-platform"
        self.password = "5gmeta-platform"


    def test_amin(self):
            # TODO: to be fixed with check connector
            # if(not stream_exists('cits_stream')):

            ksqlmeta.create_datatype_connector( self.client, self.data_type, self.name, self.activemq_url, self.username, self.password)
            # create_sink_messages_connector("events_5gmeta", "dest_tp_event", "JMS_SINK_TP1", "tcp://<ip>:61616")
            prompt_var = ""
            while prompt_var != "y":
                print(ksqlmeta.list_streams(self.client))
                print("Before querying check that the topic and the associated schema into the registry exist!")
                prompt_var = input("Continue... ? [y/n]")
            # ATTENTION: when create a stream check that the topic and the associated schema into the registry exist!
            ksqlmeta.create_stream_from_topic(self.client, "cits")
            # Then query a topic to filter it
            ksqlmeta.query_stream_to_topic(self.client, 'ksql-unitest-cits_query001', "v-1")  # 0123012301230123001
            print("Streams list: ")
            print(ksqlmeta.list_streams(self.client))


if __name__ == '__main__':
    unittest.main()
