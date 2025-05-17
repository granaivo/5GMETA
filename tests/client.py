#!/usr/bin/env python3
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

import optparse
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


# Geoposition - Next steps: from GPS device. Now hardcoded.
latitude    = 43.3128
longitude    = -1.9750

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)

## Replace with your S&D mysql database credentials
DB_USER_user="dbuser"
DB_PASSWIRD="dbpassword"
DB_HOST_ip="192.168.14.192"
DB__PORT="3307"

tile=str(tileTmp.quad_tree)
username="5gmeta-platform"
password="5gmeta-platform"

send = False
dataflowId = -1
topic = ""

timeinterval=10
keepAliveInterval=25 # time in seconds to send a new keep alive
messages=1
folder_path="sample_images"
dir_path = r'sample_images'


class Client(MessagingHandler):
    def __init__(self, url, requests):
        super(Client, self).__init__()
        self.url = url
        self.requests = requests

    def on_start(self, event):
        self.sender = event.container.create_sender(self.url)
        self.receiver = event.container.create_receiver(self.sender.connection, None, dynamic=True)

    def next_request(self):
        if self.receiver.remote_source.address:
            req = Message(reply_to=self.receiver.remote_source.address, body=self.requests[0])
            self.sender.send(req)

    def on_link_opened(self, event):
        if event.receiver == self.receiver:
            self.next_request()

    def on_message(self, event):
        print("%s => %s" % (self.requests.pop(0), event.message.body))
        if self.requests:
            self.next_request()
        else:
            event.connection.close()


REQUESTS = ["Twas brillig, and the slithy toves",
            "Did gire and gymble in the wabe.",
            "All mimsy were the borogroves,",
            "And the mome raths outgrabe."]

parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send requests to the supplied address and print responses.")
parser.add_option("-a", "--address", default="5gmeta-platform:5gmeta-platform@akkodismec.francecentral.cloudapp.azure.com:30672/cits",
                  help="address to which messages are sent (default %default)")
opts, args = parser.parse_args()

Container(Client(opts.address, args or REQUESTS)).run()
