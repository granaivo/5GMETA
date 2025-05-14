To guide integration purposes include a readme with scenario and pre-conditions, and scripts/Dockers

# Northbound examples

## Python examples

1. [nb_register_service.py](nb_register_service.py): To register a MEC to the discovery API
2. [nb_get_service_details.py](nb_get_service_details.py): Get details from a given service from a MEC
3. [nb_get_services_from_mec.py](nb_get_services_from_mec.py): Get all available services from a given MEC
4. [nb_delete_service.py](nb_delete_service.py): Delete a service from MEC
5. [nb_update_service.py](nb_update_service.py): Update a MEC service 

## curl examples

### Register a service

```
curl -X POST -H "Content-Type: application/json" -d @nb_register_service_example.json http://<ip>:<port>/v0/mecregistry/<mec_id>/nbservices
```



### Get services from MEC
```
curl -X GET -H "Content-Type: application/json" http://<ip>:<port>/v0/mecregistry/<mec_id>/nbservices
```

### Get details from service
```
curl -X GET -H "Content-Type: application/json" http://<ip>:<port>/v0/mecregistry/<mec_id>/nbservices/<service_id>
```

### Update a service 

```
curl -X PATCH -H "Content-Type: application/json" -d @nb_update_service_example.json http://<ip>:<port>/v0/mecregistry/<mec_id>/nbservices/<service_id>
```
Will answer an updated message if everything was ok:

```
{'message': 'updated', 'service_id': '1'}
```

or answer an error message  if there was something wrong:

```
{'mec_id': '1', 'message': 'SERVICE ID NOT EXISTS IN SPECIFIED MEC ID', 'service_id': '10'}
```

### Delete service
```
curl -X DELETE -H "Content-Type: application/json" http://<ip>:<por>/v0/mecregistry/<mec_id>/nbservices/<service_id>
```

That will answer 
```
{"mec_id":"1","message":"Service deleted","service_id":"1"}
```

if everything was fine and:

```
{"mec_id":"1","message":"SERVICE ID NOT EXISTS IN SPECIFIED MEC ID","service_id":"1"}
```

if there was any error.

# Southbound examples

## Python examples
There are several southbound examples to be run in python:

1. [sb_register_mec.py](sb_register_mec.py): To register a MEC to the discovery API
2. [sb_add_tile_to_mec.py](sb_add_tile_to_mec.py): To ad a tile to an existing MEC
3. [sb_get_mec_from_tile.py](sb_get_mec_from_tile.py): To get the MEC that contains the given tile
4. [sb_delete_tile.py](sb_delete_tile.py): To delete a tile from a MEC
5. [sb_delete_mec.py](sb_delete_mec.py): To delete a MEC (unregister)

## curl examples

#### Register a MEC

```
curl -X POST -H "Content-Type: application/json" -d @sb_register_example.json http://<ip>:<port>/v0/mecregistry
```

It will answer with mecid in JSON format:

```
{
  "mec_id": "1"
}
``` 

#### Add tile to MEC 

```
curl -X POST -H "Content-Type: application/json" http://<ip>:<port>/v0/mecregistry/<mec_id>/tile/<tile>
```
It will answer with an Added message with response status 200 if OK
```
{
  "message": "Added"
}
```

or  response status 400 if anything went wrong and JSON answer:
```
{
  "message": "Added"
}
```

#### Search for a MEC that contains given tile

```
curl http://<ip>:<port>/v0/mecregistry/<tile>
```

Will answer:
```
[
  {
    "geo-validity": {
      "geo-validity-tile": [
        {
          "geo-validity-tile": {
            "tile-id": "123123",
            "zoom-level": 6
          }
        }
      ]
    },
    "id": 1,
    "name": "MEC-01",
    "organization": "VICOMTECH",
    "sb_services": [
      {
        "description": "AMQP server for MEC",
        "ip": "192.168.10.9",
        "port": 5673,
        "service_name": "MessageBroker"
      },
      {
        "description": "DigitalTwin Service",
        "ip": "192.168.10.9",
        "name": "DigitalTwin",
        "port": 64000
      }
    ]
  }
]
```
or

```
[]
```
 if no MEC containing tile has not been found.



#### Delete tile from MEC

```
curl -X DELETE -H "Content-Type: application/json" http://<ip>:<port>/v0/mecregistry/<mec_id>/tile/<tile>
```

#### Delete MEC

```
curl -X DELETE -H "Content-Type: application/json" http://<ip>:<port>/v0/mecregistry/<mec_id>
```






