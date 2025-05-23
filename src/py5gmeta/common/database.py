import sqlalchemy as db
import json
import requests
from sqlalchemy.exc import SQLAlchemyError
from py5gmeta.common import api
from sqlalchemy import URL


#TODO add an JSON encoder

def to_json(model):
    return json.dumps(model, default=lambda o: json_name(o.__dict__))

def json_name(model: dict):
    dikt = {}
    for k, v in model.items():
        words = k.split('_')
        word = ""
        if len(words) > 1:  word = words.pop(0)
        for x in words:
            word = word + x.capitalize()
        k = word
        dikt[k] = v
    return dikt

class Sensor:
    def __init__(self, internal_sensor_id, senor_name, sensor_type):
        self.internal_sensor_id = internal_sensor_id
        self.sensor_name = senor_name
        self.sensor_type = sensor_type

class SourceLocationInfo:
    def __init__(self, location_quad_key: int, country: str, latitude: float, longitude: float ):
        self.location_quad_key = location_quad_key
        self.location_country = country
        self.location_latitude = latitude
        self.location_longitude = longitude

class DataSourceInfo:
    def __init__(self, source_time_zone: int, source_stratum_level: int, source_id: int, source_type: str, source_location_info: SourceLocationInfo ):
        self.source_time_zone= source_time_zone
        self.source_stratum_level = source_stratum_level
        self.source_id = source_id
        self.source_type = source_type
        self.source_location_info = source_location_info

class DataTypeInfo:
    def __init__(self, data_type: str, data_sub_type: str):
        self.data_type = data_type
        self.data_sub_type = data_sub_type

class LicenseInfo:
    def __init__(self, license_geo_limit: str, license_type: str):
        self.license_geo_limit = license_geo_limit
        self.license_type = license_type

class DataInfo:
    def __init__(self, dataformat: str, data_sample_rate: float, direction: str, extraAttributes: dict):
        self.data_format = dataformat
        self.data_sample_rate = data_sample_rate
        self.data_flow_direction = direction

class DataflowMetaData:
    def __init__(self, data_type_info: DataTypeInfo, data_info: DataInfo, license_info: LicenseInfo, data_source_info: DataSourceInfo):
        self.data_type_info = data_type_info
        self.data_info = data_info
        self.license_info = license_info
        self.data_source_info = data_source_info


class DataFlow:
    def __init__(self, dataflow_id: int, data_type_info: DataTypeInfo, data_info: DataInfo, license_info: LicenseInfo):
        self.dataflow_id = dataflow_id
        self.data_type_info = data_type_info
        self.data_info = data_info
        self.license_info = license_info

class DatabaseConnect:
    def __init__(self, username, password, db_host, db_port, db_name):
        self.engine = db.create_engine(URL.create("mysql+pymysql", username=username, password=password, host=db_host, port=db_port, database=db_name))
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.dataflows = db.Table('dataflows', self.metadata, autoload_with=self.engine)
        self.produced = db.Table('producedDataflows', self.metadata, autoload_with=self.engine)
        self.owner = db.Table('sensorOwners', self.metadata, autoload_with=self.engine)
        self.sensor = db.Table('sensorIdentity', self.metadata, autoload_with=self.engine)
        self.internal_sensor = db.Table('internalSensorIdentity', self.metadata, autoload_with=self.engine)
        self.producedDataflows = db.Table('producedDataflows', self.metadata, autoload_with=self.engine)
        self.dataflows_true = db.Table('dataflows_true', self.metadata, autoload_with=self.engine)


    def insert_dataflow_localdb(self, dataflow, owner, owner_name, owner_id):
        query = db.sql.insert(self.dataflows).values(to_json(dataflow))
        self.connection.execute(query)
        # Insert the owner in the DB
        query = db.sql.insert(owner).values({
            "ownerId": owner_id,
            "ownerName": owner_name,
        })
        try:
            self.connection.execute(query)
        except SQLAlchemyError as e:
            print(e)

    def insert_internal_sensor_local_db(self, sensor: Sensor):
        # Insert the internalSensor in the DB
        query = db.sql.insert(self.internal_sensor).values(to_json(sensor))
        try:
            self.connection.execute(query)
        except SQLAlchemyError as e:
            print(e)

    def insert_dataflow_produced_dataflows_local_db(self, produced, producer_id, dataflow_id):
        # Insert the dataflow in the producedDataflows in the DB
        query = db.sql.insert(produced).values({
            "producerDataflow": str(dataflow_id),
            "producerId": producer_id,
            "internalProducerId": 1
        })
        self.connection.execute(query)

    def keepAliveDataflow(self, url, auth_headers,  tile):
        registration_host, registration_port = api.discover_sb_service(url, tile, "registration-api", auth_headers)
        query = db.select([self.dataflows])
        results = self.connection.execute(query).fetchall()

        for result in results:
            print("Sending put for: " + result["dataflowId"], flush=True)
            r = requests.put(
                "https://" + registration_host + ":" + registration_port + "/api/v1" + "/dataflows/" + result[
                    "dataflowId"], json=({
                    "dataTypeInfo": {
                        "dataType": result["dataType"],
                        "dataSubType": result["dataSubType"]
                    },
                    "dataInfo": {
                        "dataFormat": result["dataFormat"],
                        "dataSampleRate": float(result["dataSampleRate"]),
                        "dataflowDirection": result["dataflowDirection"],
                        "extraAttributes": json.dumps(result["extraAttributes"]["dataflow"]) if result[
                            "extraAttributes"] else None,
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
                            "locationQuadkey": result["locationQuadkey"],
                            # "locationLatitude": result["locationLatitude"],
                            # "locationLongitude": result["locationLongitude"],
                            "locationCountry": result["locationCountry"],
                        }
                    }
                }))
            if r.status_code == 404:
                query = db.delete(self.producedDataflows).where(
                    [self.producedDataflows.columns.producerDataflow] == result["dataflowId"])
                self.connection.execute(query)
                query = db.delete(self.dataflows_true).where([self.dataflows_true.columns.dataflowId] == result["dataflowId"])
                self.connection.execute(query)
            self.connection.close()

    def insert_sensor_local_db(self, owner_id,  sensor):
        # Insert the sensor in the DB
        query = db.sql.insert(sensor).values(to_json(sensor))
        try:
            self.connection.execute(query)
        except:
            pass





