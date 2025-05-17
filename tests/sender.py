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
