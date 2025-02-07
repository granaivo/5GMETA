# 5GMeta Monitoring and Metering Dashboards

The 5GMeta platform allows the integration of a Prometheus service to log different time series metrics such as
resources, network and data variables. In addition, also a Grafana instance can be deployed to visualize these metrics.
Aided by these two services, a set of Grafana dashboards has been developed which allows a better visualization of the
platform metrics. There are dashboards specific for monitoring and metering the Cloud Platform and another dashboard that
helps monitor the MEC instance.

## Cloud Platform Monitoring and Metering
To monitor and meter the cloud platform two Grafana dashboards are provided:

- cloud_cluster_monitoring.json
- cloud_data_metering.json

### Monitoring 5GMeta Cloud Cluster Resources
An important aspect of a cloud platform is being able to monitor the full status of the cluster. Since the 5GMeta platform
is built on top of a Kubernetes cluster, a Grafana dashboard was implemented which takes information on the hardware and
network resources used at each moment.

The file cloud_cluster_monitoring.json can be uploaded to the 5GMeta Grafana instance in the cloud and the following panels are available:

1. Network Input/Output Pressure
This panel showcases the network data volume received and sent by the entire cluster, offering valuable insights into the data flow within the 5GMeta platform.

![Network Pressure](./images/network_pressure.jpg)

2. Total Usage
This panel provides a comprehensive overview of resource utilization, offering usage percentages for memory, CPU, and Filesystem. Additionally, it furnishes total resource values within the Cloud platform alongside absolute usage figures, facilitating a thorough understanding of system performance and resource allocation.

![Total Usage](./images/total_usage.jpg)

3. All process CPU usage
This panel illustrates the progression of CPU usage over time, with the graph depicting the total cores utilized by all processes within the cluster.

![CPU Usage](./images/cpu_usage.jpg)

4. All process memory usage
This panel illustrates the progression of Memory usage over time, with the graph depicting the total GB utilized by all processes within the cluster.

![Memory Usage](./images/memory_usage.jpg)

### Metering 5GMeta Cloud Data Volume by Topic
With the Prometheus metric logging system, it is possible to access crucial insights into the total data volume transmitted via the 5GMeta Platform. This data volume is accessible through the platform's created data topics, enabling various uses, including user data volume monetization.

The created dashboard can be seamlessly imported into the 5GMeta Grafana instance in the cloud using the file: cloud_data_metering.json.

This dashboard contains two panels:

1. Data Volume Evolution
This panel illustrates the cumulative progression of data transmitted by a dataflow topic over time. The dropdown options enable the selection of the user, data type, and dataflow topic. Multiple values can be chosen to visualize the accumulated data transmitted from the data producer to the data consumer.

2. Total Data Transmitted
This panel illustrates the total volume transmitted by the selected combination of users, data type and dataflow topic.

![Data Volume](./images/data_volume.jpg)

## MEC Instance Monitoring
The various MEC instances facilitate Prometheus and Grafana configuration for metrics logging, enabling the extraction of vital information on MEC resource usage in real-time. To facilitate this, we offer a Grafana dashboard that retrieves data from Prometheus, displaying resource usage metrics. This dashboard mirrors the [monitoring dashboard](#monitoring-5gmeta-cloud-cluster-resources) designed for the 5GMeta Cloud Platform, featuring identical panels for consistency.

1. Network Input/Output Pressure
This panel showcases the network data volume received and sent by the entire cluster, offering valuable insights into the data flow within the 5GMeta platform.

2. Total Usage
This panel provides a comprehensive overview of resource utilization, offering usage percentages for memory, CPU, and Filesystem. Additionally, it furnishes total resource values within the Cloud platform alongside absolute usage figures, facilitating a thorough understanding of system performance and resource allocation.

3. All process CPU usage
This panel illustrates the progression of CPU usage over time, with the graph depicting the total cores utilized by all processes within the cluster.

4. All process memory usage
This panel illustrates the progression of Memory usage over time, with the graph depicting the total GB utilized by all processes within the cluster.