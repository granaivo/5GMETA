#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from __future__ import print_function
from threading import Thread
from proton.handlers import MessagingHandler
from proton.reactor import Container
from pygeotile.tile import Tile
from proton.reactor import Container
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

    def receeive():
        pass



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

    def send():
        passs


def sendKeepAlive(dataflowmetadata, dataflowId):
    global send
    while(True):
        time.sleep(30)
        r= api.keepAliveDataflow(dataflowmetadata,dataflowId)
        print(r.text)
        send = r.json()['send']


def send(url, boqy, dataflowmetadata, platformaddress, registrationapi_port, amqp_port ):


    # Send the JSON of the dataflow's metadata, and receive the dataflowId and the topic where to publish the messages
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
                Container(Sender("amqp://<username>:<password>@"+platformaddress+":"+amqp_port+":/topic://"+topic, content.messages)).run()
                print("Message sent.\n")
        except KeyboardInterrupt:
            exit(1)
        time.sleep(int(1))



def gen_metadata(datatype, subtype, dataformat, direction, country, latitude, longitude):
    # Replace with your metadata
    tile=get_tile(latitude,longitude)
    dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": datatype,
        "dataSubType": subtype
    },
    "dataInfo": {
        "dataFormat": dataformat,
        "dataSampleRate": 0.0,
        "dataflowDirection": direction,
        "extraAttributes": None,
    },
    "licenseInfo": {
        "licenseGeolimit": "europe",
        "licenseType": "profit"
    },
    "dataSourceInfo": {
        "sourceTimezone": 2,
        "sourceStratumLevel": 3,
        "sourceId": 1,
        "sourceType": "vehicle",
        "sourceLocationInfo": {
            "locationQuadkey": tile,
            "locationCountry": country,
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }
    }
    return dataflowmetadata


def gen_moving_gps(latitude,longitude):

    # Pseudo movement to get different MEC coverage

    lat_rand=random.uniform(0.001,-0.02)
    long_rand=random.uniform(0.01,-0.01)

    latitudeRand=latitude+lat_rand
    longitudeRand=longitude+long_rand
    return latitudeRand, longitudeRand
