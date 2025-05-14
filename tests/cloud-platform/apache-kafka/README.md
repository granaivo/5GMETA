# Confluentic Apache Kafka

## Overview
This project contains all the components and connectors necessary to retrieve data from MEC data pipelines and make them accessible to consumers in internet through the 5GMETA Cloud platform.

*NOTE*: If you want to use the Keycloak-based topic autorization feature, please switch to the *with-keycloack-authorization* branch.

Thus, the Stream Data Gatawey module contains:

- Conluentic Apache Kafka
- Connectors configuration to retrive the messages from ActiveMQ (src/)
- An example of Kafka Consumer (examples/)
    - consumer

![Sequence Diagram of exchanged Messages](miscelania/seqdiag.png)

## Deployment of the Cloud data bus
The deployment process of the 5GMETA Cloud data bus based on Kafka is described in [deploy](https://github.com/5gmeta/stream-data-gateway/tree/main/deploy) folder.


## Authors
- Mikel Serón Esnal ([mseron@vicomtech.org](mailto:mseron@vicomtech.org), [GitHub](https://github.com/mikelseron))
- Felipe Mogollón ([fmogollon@vicomtech.org](mailto:fmogollon@vicomtech.org))

## License

Copyright : Copyright 2022 VICOMTECH

License : EUPL 1.2 ([https://eupl.eu/1.2/en/](https://eupl.eu/1.2/en/))

The European Union Public Licence (EUPL) is a copyleft free/open source software license created on the initiative of and approved by the European Commission in 23 official languages of the European Union.

Licensed under the EUPL License, Version 1.2 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at [https://eupl.eu/1.2/en/](https://eupl.eu/1.2/en/)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
