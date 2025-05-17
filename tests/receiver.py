latitude    = 43.2952
longitude    = -1.9850

tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
tile=str(tileTmp.quad_tree)
print(tile)
username="<username>"
password="<password>"

# Get Message Broker access
service="message-broker"
messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile,service)
if messageBroker_ip == -1 or messageBroker_port == -1:
    print(service+" service not found")
    exit(-1)



url ="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+"/topic://event"




if __name__ == "__main__":
    try:
        Container(Receiver(url)).run()
    except KeyboardInterrupt:
        pass
