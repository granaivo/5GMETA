# 5GMETA Platform Client
Third party pyhton client for making requests to 5GMETA Platform.
Every user must to sign in, choose the tiles where wants to consume data and for every tile choose the datatype and the instance type.
For omitting instance type election run the client with '--disable-instanceapi' option.

## Requeriments
- Python3
- Registered user in the platform. To register use: [5GMETA User Registration](<5gmeta-ip>/identity/realms/5gmeta/account/)

## How to use
- Clone the content of [platform-client](https://github.com/5gmeta/stream-data-gateway/tree/main/utils/platform-client) folder
- Run 'python3 client.py'
