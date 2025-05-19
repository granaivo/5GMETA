import sqlalchemy as db
import json
import requests
from py5gmeta.common import api

def insert_dataflow_localdb(dataflow_metadata, connection, dataflows, owner, owner_name, owner_id, dataflow_id):
    # Insert the dataflow in the DB
    query = db.sql.insert(dataflows).values({
        "dataflowId": str(dataflow_id),
        "dataType": dataflow_metadata['dataTypeInfo']["dataType"],
        "dataSubType": dataflow_metadata['dataTypeInfo']["dataSubType"],
        "dataFormat": dataflow_metadata['dataInfo']["dataFormat"],
        "dataSampleRate": dataflow_metadata['dataInfo']["dataSampleRate"],
        "licenseGeolimit": dataflow_metadata['licenseInfo']["licenseGeolimit"],
        "licenseType": dataflow_metadata['licenseInfo']["licenseType"],
        "dataflowAttributes": json.loads(dataflow_metadata['dataInfo']["extraAttributes"].replace("\'", "\"")) if dataflow_metadata['dataInfo']["extraAttributes"] else None,
        "dataflowDirection": dataflow_metadata['dataInfo']["dataflowDirection"]
    })
    connection.execute(query)

    # Insert the owner in the DB
    query = db.sql.insert(owner).values({
        "ownerId": owner_id,
        "ownerName": owner_name,
    })
    try:
        connection.execute(query)
    except:
        pass


def insert_internal_sensor_local_db(connection, internal_sensor, sensor_name, sensor_type):
    # Insert the internalSensor in the DB
    query = db.sql.insert(internal_sensor).values({
        "internalSensorId": 1,
        "sourceId": "1",
        "internalSensorName": sensor_name,
        "internalSensortype": sensor_type
    })
    try:
        connection.execute(query)
    except:
        pass

def insert_dataflow_produced_dataflows_local_db(connection, produced, producer_id, dataflow_id):
    # Insert the dataflow in the producedDataflows in the DB
    query = db.sql.insert(produced).values({
        "producerDataflow": str(dataflow_id),
        "producerId": producer_id,
        "internalProducerId": 1
    })
    connection.execute(query)


def prepare_database(db_user,db_password,db_ip,db_port):
    engine = db.create_engine('mysql+pymysql://'+db_user+':'+db_password+'@'+db_ip+':+'+db_port+'/5GMETA_SD', isolation_level="READ UNCOMMITTED")
    connection = engine.connect()
    metadata = db.MetaData()
    dataflows = db.Table('dataflows', metadata,  autoload_with=engine)
    produced = db.Table('producedDataflows', metadata,  autoload_with=engine)
    owner = db.Table('sensorOwners', metadata,  autoload_with=engine)
    sensor = db.Table('sensorIdentity', metadata,  autoload_with=engine)
    internal_sensor =  db.Table('internalSensorIdentity', metadata, autoload_with=engine)
    return connection, dataflows,produced,owner,sensor,internal_sensor


def keepAliveDataflow(db_ip,db_user,db_password,db_port,tile):

    registration_ip, registration_port = api.discover_sb_service(tile,"registration-api")

    engine = db.create_engine('mysql+pymysql://'+db_user+':'+db_password+'@'+db_ip+':'+db_port+'/5GMETA_SD', isolation_level="READ UNCOMMITTED")
    connection = engine.connect()
    metadata = db.MetaData()
    dataflows = db.Table('extendedDataflows', metadata, autoload=True, autoload_with=engine)
    dataflows_true = db.Table('dataflows', metadata, autoload=True, autoload_with=engine)
    producedDataflows = db.Table('producedDataflows', metadata, autoload=True, autoload_with=engine)
    query = db.select([dataflows])
    results = connection.execute(query).fetchall()
    for result in results:
        print("Sending put for: "+result["dataflowId"], flush=True)
        r = requests.put("https://"+registration_ip+":"+registration_port+ "/api/v1" + "/dataflows/"+result["dataflowId"], json = ({
            "dataTypeInfo": {
                "dataType": result["dataType"],
                "dataSubType": result["dataSubType"]
            },
            "dataInfo": {
                "dataFormat": result["dataFormat"],
                "dataSampleRate": float(result["dataSampleRate"]),
                "dataflowDirection": result["dataflowDirection"],
                "extraAttributes": json.dumps(result["extraAttributes"]["dataflow"]) if result["extraAttributes"] else None,
            },
            "licenseInfo": {
                "licenseGeolimit": result["licenseGeolimit"],
                "licenseType": result["licenseType"]
            },
            "dataSourceInfo": {
                "timeZone": int(result["timeZone"]) if result["timeZone"] else None,
                "timeStratumLevel": int(result["timeStratumLevel"]) if result["timeStratumLevel"] else None,
                "sourceId": int(result["sourceId"]),
                "sourceType": result['sourceType'],
                "sourceLocationInfo": {
                    "locationQuadkey":result["locationQuadkey"],
                    # "locationLatitude": result["locationLatitude"],
                    # "locationLongitude": result["locationLongitude"],
                    "locationCountry": result["locationCountry"],
                }
            }
        }))
        if(r.status_code == 404):
            query = db.delete(producedDataflows).where(producedDataflows.columns.producerDataflow == result["dataflowId"])
            connection.execute(query)
            query = db.delete(dataflows_true).where(dataflows_true.columns.dataflowId == result["dataflowId"])
            connection.execute(query)
        connection.close()



def insert_sensor_local_db(owner_id, vehicle_name, dataflowmetadata, connection, sensor):
    # Insert the sensor in the DB
    query = db.sql.insert(sensor).values({
        "sourceId": dataflowmetadata['dataSourceInfo']["sourceId"],
        "ownerId": owner_id,
        "sourceName": vehicle_name,
        "sourceType": dataflowmetadata['dataSourceInfo']["sourceType"],
        "locationQuadkey": dataflowmetadata['dataSourceInfo']["sourceLocationInfo"]["locationQuadkey"],
        "locationCountry": dataflowmetadata['dataSourceInfo']["sourceLocationInfo"]["locationCountry"],
        "timeZone": dataflowmetadata['dataSourceInfo']["sourceTimezone"],
        "timeStratumLevel": dataflowmetadata['dataSourceInfo']["sourceStratumLevel"],
    })
    try:
        connection.execute(query)
    except:
        pass
