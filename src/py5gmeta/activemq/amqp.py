from __future__ import print_function
from threading import Thread
import requests
from proton.handlers import MessagingHandler
from proton.reactor import Container
from py5gmeta.common import api


class Receiver(MessagingHandler):
    def __init__(self, url, messages_to_receive=10):
        super(Receiver, self).__init__()
        self.url = url
        self._messages_to_receive = messages_to_receive
        self._messages_actually_received = 0
        self._stopping = False

    def on_start(self, event):
        event.container.create_receiver(self.url)

    def on_message(self, event):
        if self._stopping:
            return

        print(event.message.body)
        self._messages_actually_received += 1
        if self._messages_actually_received == self._messages_to_receive:
            event.connection.close()
            self._stopping = True

    def on_transport_error(self, event):
        raise Exception(event.transport.condition)


class Sender(MessagingHandler):
    def __init__(self, url, messages):
        super(Sender, self).__init__()
        self.url = url
        self._messages = messages
        self._message_index = 0
        self._sent_count = 0
        self._confirmed_count = 0

    def on_start(self, event):
        event.container.create_sender(self.url)

    def on_sendable(self, event):
        while event.sender.credit and self._sent_count < len(self._messages):
            message = self._messages[self._message_index]
            #print("Send to "+ self.url +": \n\t" )#+ str(message))
            #print(str(message))
            event.sender.send(message)
            self._message_index += 1
            self._sent_count += 1
            event.sender.close()

    def on_accepted(self, event):
        self._confirmed_count += 1
        if self._confirmed_count == len(self._messages):
            event.connection.close()

    def on_transport_error(self, event):
        raise Exception(event.transport.condition)


# Send the JSON of the dataflow's metadata, and receive the dataflowId and the topic where to publish the messages

def send_with_keep_alive(url, topic, body, dataflow_metadata, auth_headers ):
    # Start sending keepalives
    # Start publishing messages in the received topic
    dataflow_id = ""
    r = requests.post(url, json=dataflow_metadata)
    if r.status_code == 200:
        r = r.json()
        dataflow_id = r['id']
        topic = r['topic']
        send = r['send']
    else:
        print(r.text)
        exit()
    def send_keep_alive():
        api.keep_alive_dataflow(url, dataflow_metadata, dataflow_id, auth_headers)

    thread = Thread(target=send_keep_alive)
    thread.start()

#TODO add description of content
def send(url, topic, content):
        try:
            Container(Sender(url + ":/topic://" + topic, content)).run()
            print("Message sent.\n")
        except Exception as e:
            print(e)


def receive(amqp_server_url, topic, message_to_receive):
    try:
        Container(Receiver(amqp_server_url+ ":/topic://" + topic, message_to_receive)).run()
    except KeyboardInterrupt:
        pass