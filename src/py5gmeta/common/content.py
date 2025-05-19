import string

from proton import Message, symbol, ulong, PropertyDict
import base64
import sys
import random
import time

#messages = [Message(subject='s%d' % i, body=u'b%d' % i) for i in range(10)]

body = '{"header":{"protocolVersion":2,"messageID":2,"stationID":3907},"cam":{"generationDeltaTime":2728,"camParameters":{"basicContainer":{"stationType":5,"referencePosition":{"latitude":435549160,"longitude":103036950,"positionConfidenceEllipse":{"semiMajorConfidence":4095,"semiMinorConfidence":4095,"semiMajorOrientation":3601},"altitude":{"altitudeValue":180,"altitudeConfidence":"unavailable"}}},"highFrequencyContainer":{"basicVehicleContainerHighFrequency":{"heading":{"headingValue":1340,"headingConfidence":127},"speed":{"speedValue":618,"speedConfidence":127},"driveDirection":"unavailable","vehicleLength":{"vehicleLengthValue":42,"vehicleLengthConfidenceIndication":"unavailable"},"vehicleWidth":20,"longitudinalAcceleration":{"longitudinalAccelerationValue":161,"longitudinalAccelerationConfidence":102},"curvature":{"curvatureValue":359,"curvatureConfidence":"unavailable"},"curvatureCalculationMode":"yawRateUsed","yawRate":{"yawRateValue":1,"yawRateConfidence":"unavailable"},"accelerationControl":"00","lanePosition":-1}},"lowFrequencyContainer":{"basicVehicleContainerLowFrequency":{"vehicleRole":"default","exteriorLights":"00","pathHistory":[{"pathPosition":{"deltaLatitude":-280,"deltaLongitude":1140,"deltaAltitude":250},"pathDeltaTime":22393}]}}}}}'


# https://stackoverflow.com/questions/2511222/efficiently-generate-a-16-character-alphanumeric-string
def generate_random_group_id (length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def messages_generator(num, msgbody):
    messages = []
    print("Sender prepare the messages... ")
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


def cits_messages_generator(num, tile, msgbody=body, dataflowId=1):
    messages = []
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
    return messages


def image_messages_generator(image, num, tile, msgbody=None ):
    messages = []
    if not msgbody :
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
    return messages