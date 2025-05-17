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
#
#
# Author: D.Amendola
# Useful resources: 
#   https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/python/docs/tutorial.html
#   https://access.redhat.com/documentation/en-us/red_hat_amq/6.3/html/client_connectivity_guide/amqppython

from __future__ import print_function

import optparse
import json
import time
from proton.handlers import MessagingHandler
from proton.reactor import Container
import sd_database
import py5gmeta.common.content
import py5gmeta.common.api

from pygeotile.tile import Tile
import os
from proton.reactor import Container


import sd_database


import content_folder
import random


import address
import content

import sqlalchemy as db
import requests

from threading import Thread








latitude    = 43.361534
longitude    = 1.987018

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
username="5gmeta-platform"
password="5gmeta-platform"

send = False
dataflowId = -1
topic = ""

def sendKeepAlive():
    global send
    while(True):
        time.sleep(30)
        r = requests.put("http://"+platformaddress+':12346/dataflows/'+str(dataflowId), json = (dataflowmetadata))
        print(r.text)
        send = r.json()['send']

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
            print("Send to "+ self.url +": \n\t" )#+ str(message))
            event.sender.send(message)
            self._message_index += 1
            self._sent_count += 1

    def on_accepted(self, event):
        self._confirmed_count += 1
        if self._confirmed_count == len(self._messages):
            event.connection.close()

    def on_transport_error(self, event):
        raise Exception(event.transport.condition)


platformaddress = "192.168.85.151"
registrationapi_port = "12346"
amqp_port = "5673"

dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "cits",
        "dataSubType": "cam"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
        "extraAttributes": None,
    },
    "licenseInfo": {
        "licenseGeolimit": "europe",
        "licenseType": "profit"
    },
    "dataSourceInfo": {
        "timeZone": 10,
        "timeStratumLevel": 3,
        "sourceId": 1,
        "sourceType": "vehicle",
        "sourceLocationInfo": {
            "locationQuadkey": str(12022301011102),
            "locationCountry": "ITA",
        }
    }   
}

body = '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'

if __name__ == "__main__":

    # Url to add a dataflow
    url = "http://"+platformaddress+':'+registrationapi_port+'/dataflows'

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



#--- CITS
# Geoposition - Next steps: from GPS device. Now hardcoded.

#latitude=43.664858
#longitude=1.353251
latitude=43.361534
longitude=1.987018

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
username="5gmeta-platform"
password="5gmeta-platform"

# Replace with your metadata
dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "cits",
        "dataSubType": "cam"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
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
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }
}
def sendKeepAlive():
    global send
    while(True):
        time.sleep(30)
        r= discovery_registration.keepAliveDataflow(dataflowmetadata,dataflowId)
        #print(r.text)
        send = r.json()['send']



body = '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'


class CITSSender(MessagingHandler):
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



if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send messages to the supplied address.")

    parser.add_option("-a", "--address",
                    help="address to which messages are sent (default %default)")

    parser.add_option("-m", "--messages", type="int", default=1,
                    help="number of messages to send (default %default)")

    parser.add_option("-t", "--timeinterval", default=10,
                    help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")


    opts, args = parser.parse_args()

    # Get Message Broker access
    service="message-broker"
    tile="1202022213220223"
    print(tile)
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)

    messageBroker_port = 30673
    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic,send = discovery_registration.register(dataflowmetadata,tile)

    opts.address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+":/topic://"+topic

    jargs = json.dumps(args)

    print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(opts.address) + "\n" )

    # generate message
    content.messages_generator(opts.messages,tile,body,dataflowId)


    #Start sending keepalives
    thread = Thread(target = sendKeepAlive)
    thread.start()

    # send message
    while(True):
        try:
            print(tile)
            #print(content.messages)
            if(send):
                print("Send received, send data")
                Container(Sender(opts.address, content.messages)).run()
                print("... \n")
        except KeyboardInterrupt:
            pass
        if (int(opts.timeinterval) == 0):
            break
        time.sleep(int(opts.timeinterval))


class ImageSender(MessagingHandler):
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


if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send messages to the supplied address.")

    parser.add_option("-a", "--address",
                    help="address to which messages are sent (default %default)")

    parser.add_option("-m", "--messages", type="int", default=1,
                    help="number of messages to send (default %default)")

    parser.add_option("-t", "--timeinterval", default=10,
                    help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")



    # Get Message Broker access
    service="message-broker"
    tile = "1202022213220223"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)

    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic,send = discovery_registration.register(dataflowmetadata,tile)


    print("dataflow id is "+str(dataflowId))


    opts, args = parser.parse_args()
    messageBroker_port = 31888
    opts.address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+"/topic://"+topic


    jargs = json.dumps(args)

    print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(opts.address) + "\n" )

    # generate message
    content.messages_generator(opts.messages,tile)

    # send message
    while(True):
        try:
            if(send):
                print("Send received, send data")
                Container(Sender(opts.address, content.messages)).run()
                discovery_registration.keepAliveDataflow(dataflowmetadata,dataflowId)

            print("... \n")
        except KeyboardInterrupt:
            pass
        if (int(opts.timeinterval) == 0):
            break
        time.sleep(int(opts.timeinterval))



## Send Moving location

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




def get_tile(latitude, longitude,zoom=18):
    tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=zoom)
    tile=str(tileTmp.quad_tree)
    return tile


def gen_metadata(latitude,longitude):
    # Replace with your metadata
    tile=get_tile(latitude,longitude)
    dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "cits",
        "dataSubType": "cam"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
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
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }
    }
    return dataflowmetadata


def get_message_broker_address(tile):
    # Get Message Broker access
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        return -1

    address="amqp://"+username+":"+password+"@"+str(messageBroker_ip)+":"+str(messageBroker_port)+":/topic://"+topic
    return address

def gen_moving_gps(latitude,longitude):

    # Pseudo movement to get different MEC coverage

    lat_rand=random.uniform(0.001,-0.02)
    long_rand=random.uniform(0.01,-0.01)

    latitudeRand=latitude+lat_rand
    longitudeRand=longitude+long_rand
    return latitudeRand, longitudeRand

if __name__ == "__main__":


    # Geoposition - Next steps: from GPS device. Now hardcoded.
    # Geoposition tuned for Vicomtech MEC
    latitude    = 43.2924
    longitude    = -1.9861

    username="<username>"
    password="<password>"

    timeinterval=10

    tile= get_tile(latitude, longitude)
    print(tile)

    dataflowmetadata=gen_metadata(latitude,longitude)

    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic = discovery_registration.register(dataflowmetadata,tile)

    address = get_message_broker_address(tile)

    # generate message
    content.messages_generator(1,tile)

    # send message
    while(True):
        try:
            lat,lon=gen_moving_gps(latitude,longitude)
            randTile=get_tile(lat,lon,20)
            dataflowmetadata=gen_metadata(lat,lon)
            address = get_message_broker_address(randTile)
            if address != -1:

                # AMQP address is sent as payload in order to identify which MEC has been used to push the data
                content.messages_generator(1,randTile,"Other Tile "+address)
                try:
                    Container(Sender(address, content.messages)).run()
                    discovery_registration.keepAliveDataflow(dataflowmetadata,dataflowId)
                except:
                    print("Cannot connect to "+address)
            else:
                print("MEC not found, do not send anything")
            print("... \n")
        except KeyboardInterrupt:
            pass
        if (int(timeinterval) == 0):
            break
        time.sleep(int(timeinterval))





# Geoposition - Next steps: from GPS device. Now hardcoded.
# Put your position here
latitude    = 43.2952
longitude    = -1.9850

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
username="<username>"
password="<password>"
timeinterval=10
keepAliveInterval=25 # time in seconds to send a new keep alive
messages=1
folder_path="sample_images"
dir_path = r'sample_images'



# Replace with your metadata
dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "image",
        "dataSubType": "cam"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
        "extraAttributes": None,
    },
    "licenseInfo": {
        "licenseGeolimit": "europe",
        "licenseType": "profit"
    },
    "dataSourceInfo": {
        "sourceTimezone": 2,
        "sourceStratumLevel": 1,
        "sourceId": 1,
        "sourceType": "vehicle",
        "sourceLocationInfo": {
            "locationQuadkey": tile,
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }
}


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




if __name__ == "__main__":

    # Get Message Broker access
    print(tile)
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)

    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic = discovery_registration.register(dataflowmetadata,tile)

    print("dataflow id is "+str(dataflowId))

    address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+"/topic://"+topic



    print("Sending #" + str(1) + " messages every " + str(timeinterval) + " seconds to: " + str(address) + "\n" )
    keepAliveTime= int( time.time() )
    print(keepAliveTime)

    for path in os.listdir(dir_path):
        # check if current path is a file
        if os.path.isfile(os.path.join(dir_path, path)):
            content_folder.messages_generator(messages,tile,"./"+folder_path+"/"+path)
            try:
                Container(Sender(address, content_folder.messages)).run()
                now = int( time.time() )
                print(now)
                if ((now  - keepAliveTime) > keepAliveInterval):
                    print("New keep alive")
                    discovery_registration.keepAliveDataflow(dataflowmetadata,dataflowId)
                    keepAliveTime = now
                print("... \n")
            except KeyboardInterrupt:
                pass
            if (int(timeinterval) == 0):
                break
            time.sleep(int(timeinterval))





# Geoposition - Next steps: from GPS device. Now hardcoded.
latitude    = 43.3128
longitude    = -1.9750

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
username="<username>"
password="<password>"


## Replace with your S&D mysql database credentials
db_user="dbuser"
db_password="dbpassword"
db_ip="192.168.14.192"
db_port="3307"


# Replace with your metadata
dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "image",
        "dataSubType": "cam"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
        "extraAttributes": None,
    },
    "licenseInfo": {
        "licenseGeolimit": "europe",
        "licenseType": "profit"
    },
    "dataSourceInfo": {
        "sourceTimezone": 2,
        "sourceStratumLevel": 1,
        "sourceId": 1,
        "sourceType": "vehicle",
        "sourceLocationInfo": {
            "locationQuadkey": tile,
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }   
}


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




if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send messages to the supplied address.")
    
    parser.add_option("-a", "--address",
                    help="address to which messages are sent (default %default)")

    parser.add_option("-m", "--messages", type="int", default=1,
                    help="number of messages to send (default %default)")

    parser.add_option("-t", "--timeinterval", default=10,
                    help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")



    # Get Message Broker access
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)
    
    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic = discovery_registration.register(dataflowmetadata,tile)

    print("dataflow id is "+str(dataflowId))


    # Insert data into Sensor and Device database
    connection, dataflows,produced,owner,sensor,internalSensor = sd_database.prepare_database(db_user,db_password,db_ip,db_port)
    sd_database.insert_dataflow_localdb(dataflowmetadata, connection, dataflows, owner, dataflowId)
    sd_database.insert_sensor_local_db(dataflowmetadata, connection, sensor)
    sd_database.insert_internal_sensor_local_db(connection, internalSensor)
    sd_database.insert_dataflow_produced_dataflows_local_db(connection, produced, dataflowId)

    opts, args = parser.parse_args()

    opts.address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+"/topic://"+topic

    
    jargs = json.dumps(args)

    print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(opts.address) + "\n" )

    # generate message
    content.messages_generator(opts.messages,tile)

    # send message 
    while(True):
        try:
            Container(Sender(opts.address, content.messages)).run()
            sd_database.keepAliveDataflow(db_ip,db_user,db_password,db_port,tile)
            print("... \n")
        except KeyboardInterrupt:
            pass
        if (int(opts.timeinterval) == 0):
            break
        time.sleep(int(opts.timeinterval))#



# Geoposition - Next steps: from GPS device. Now hardcoded.
latitude    = 43.3128
longitude    = -1.9750

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
username="<username>"
password="<password>"

## Replace with your S&D mysql database credentials
db_user="dbuser"
db_password="dbpassword"
db_ip="192.168.14.192"
db_port="3307"


# Replace with your metadata
dataflowmetadata = {
    "dataTypeInfo": {
        "dataType": "cits",
        "dataSubType": "json"
    },
    "dataInfo": {
        "dataFormat": "asn1_jer",
        "dataSampleRate": 0.0,
        "dataflowDirection": "upload",
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
            "locationCountry": "ESP",
            "locationLatitude": latitude,
            "locationLongitude": longitude
        }
    }   
}

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



if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send messages to the supplied address.")
    
    parser.add_option("-a", "--address",
                    help="address to which messages are sent (default %default)")

    parser.add_option("-m", "--messages", type="int", default=100,
                    help="number of messages to send (default %default)")

    parser.add_option("-t", "--timeinterval", default=10,
                    help="messages are sent continuosly every time interval seconds (0: send once) (default %default)")


    opts, args = parser.parse_args()

    # Get Message Broker access
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)
    
    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic = discovery_registration.register(dataflowmetadata,tile)


    # Insert data into Sensor and Device database
    connection, dataflows,produced,owner,sensor,internalSensor = sd_database.prepare_database(db_user,db_password,db_ip,db_port)
    sd_database.insert_dataflow_localdb(dataflowmetadata, connection, dataflows, owner, dataflowId)
    sd_database.insert_sensor_local_db(dataflowmetadata, connection, sensor)
    sd_database.insert_internal_sensor_local_db(connection, internalSensor)
    sd_database.insert_dataflow_produced_dataflows_local_db(connection, produced, dataflowId)


    opts.address="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+":/topic://"+topic

    jargs = json.dumps(args)

    print("Sending #" + str(opts.messages) + " messages every " + str(opts.timeinterval) + " seconds to: " + str(opts.address) + "\n" )

    # Generate message
    content.messages_generator(opts.messages,tile)

    # send message
    while(True):
        try:
            Container(Sender(opts.address, content.messages)).run()
            sd_database.keepAliveDataflow(db_ip,db_user,db_password,db_port,tile)
            print("... \n")
        except KeyboardInterrupt:
            pass
        if (int(opts.timeinterval) == 0):
            break
        time.sleep(int(opts.timeinterval))
