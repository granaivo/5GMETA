from __future__ import print_function
from threading import Thread
from proton.handlers import MessagingHandler
from proton.reactor import Container
from pygeotile.tile import Tile
from  py5gmeta.common import database, content, api, address, content
import optparse
import json
import time
import codecs
import os
import random
import sqlalchemy as db
import requests



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

        print(event.message)
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



def sendKeepAlive(dataflowmetadata, dataflowId):
    global send
    while(True):
        time.sleep(30)
        r= api.keepAliveDataflow(dataflowmetadata,dataflowId)
        print(r.text)
        send = r.json()['send']


# Send the JSON of the dataflow's metadata, and receive the dataflowId and the topic where to publish the messages

def send(url, topic, body, dataflowmetadata ):

    r = requests.post(url, json = dataflowmetadata)
    if(r.status_code == 200):
        r = r.json()
        dataflowId = r['id']
        topic = r['topic']
        send = r['send']
    else:
        print(r.text)
        exit()

    #Start sending keepalives
    thread = Thread(target = sendKeepAlive)
    thread.start()

    # Start publishing messages in the received topic
    while(True):
        try:
            #If need to send, send message every second
            if(send):
                content.messages_generator(1, body)
                Container(Sender(url + ":/topic://" + topic, content.messages)).run()
                print("Message sent.\n")
        except KeyboardInterrupt:
            exit(1)
        time.sleep(int(1))




