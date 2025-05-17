from proton import Message
from proton import symbol, ulong, PropertyDict
import base64
import sys
import random

messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(10)]


def messages_generator(num, msgbody):
    messages.clear()

    print("Sender prepare the messages... ")
    import time
    random.seed(time.time()*1000)
    for i in range(num):        
        props = {
                    "dataType": "cits", 
                    "dataSubType": "cam", 
                    "dataFormat": "asn1_jer",
                    "sourceId": 1, 
                    "locationQuadkey": "12022301011102", 
                    "timestamp": time.time()*1000,
                    "body_size": str(sys.getsizeof(msgbody))
                    }
        messages.append(Message(body= msgbody, properties=props))
        #print(messages[i])

    print("Message array done! \n")
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
# Mods by Danilo Amendola
#
# https://qpid.apache.org/releases/qpid-proton-0.31.0/proton/python/docs/proton.html#proton.PropertyDict

from proton import Message
from proton import symbol, ulong, PropertyDict
import base64
import sys

messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(1)]

#
# author: DAmendola
# This method creates a list of messages that will be sent to the Message Broker.
#
body = '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'

def messages_generator(num, tile, msgbody=body, dataflowId=1):
    messages.clear()
    
    #print("Sender prepare the messages... ")
    for i in range(num):        
        props = {
                    "dataType": "cits",
                    "dataSubType": "cam",
                    "dataFlowId": dataflowId,
                    "dataFormat":"asn1_jer",
                    "sourceId": 1,
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msgbody))
                    }

        messages.append( Message(body=msgbody, properties=props) )
        #print(messages[i])

    #print("Message array done! \n")
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
# Mods by Danilo Amendola
#
# https://qpid.apache.org/releases/qpid-proton-0.31.0/proton/python/docs/proton.html#proton.PropertyDict

from proton import Message
from proton import symbol, ulong, PropertyDict
import base64
import sys

messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(10)]

#
# author: DAmendola
# This method creates a list of messages that will be sent to the Message Broker.
#
def messages_generator(num, tile, msgbody=None ):
    messages.clear()

    image = "sample_images/Sample_1.jpg" # MAX SIZE 15MB
    
    if(msgbody==None):
        with open(image, "rb") as f:
            msgbody = base64.b64encode(f.read())
    
    print("Size of the image: " + str(sys.getsizeof(msgbody)))

    print("Sender prepare the messages... ")
    for i in range(num):        
        props = {
                    "dataType": "image", 
                    "dataSubType": "jpg", 
                    "sourceId": "v"+str(i),
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msgbody))
                }

        messages.append( Message(body=msgbody, properties=props) ) 
        #print(messages[i])
    print("Message array done! \n")
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
# Mods by Danilo Amendola
#
# https://qpid.apache.org/releases/qpid-proton-0.31.0/proton/python/docs/proton.html#proton.PropertyDict

from proton import Message
from proton import symbol, ulong, PropertyDict
import base64
import sys

messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(10)]

#
# author: DAmendola
# This method creates a list of messages that will be sent to the Message Broker.
#
def messages_generator(num, tile, image, msgbody=None ):
    messages.clear()

    
    if(msgbody==None):
        with open(image, "rb") as f:
            msgbody = base64.b64encode(f.read())
    
    print("Size of the image: " + str(sys.getsizeof(msgbody)))

    print("Sender prepare the messages... ")
    for i in range(num):        
        props = {
                    "dataType": "image", 
                    "dataSubType": "jpg", 
                    "sourceId": "v"+str(i),
                    "locationQuadkey": tile+str(i%4),
                    "body_size": str(sys.getsizeof(msgbody))
                }

        messages.append( Message(body=msgbody, properties=props) ) 
        #print(messages[i])
    print("Message array done! \n")
