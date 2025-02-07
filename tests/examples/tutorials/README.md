# Tutorial for processing data with 5GMETA platform

This page introduces basic example for the processing of data gathered from 5GMETA platform.
These tutorials can be found in [examples/tutorials](https://github.com/5gmeta/5gmeta-dev/tree/main/examples/tutorials) directory.

## Requirements

* python3
* matplotlib (pip install matplotlib)
* folium (pip install folium)

Also find easy installation for all the required packages with [requirements.txt](https://github.com/5gmeta/5gmeta-dev/blob/main/examples/tutorials/requirements.txt) file (Be careful of your environment compatibility):

* ```pip3 install -r examples/tutorials/requirements.txt```

## Example 1: Parallel processing of data consumed

In this example, we show how data can be consumed in a dedicated thread and a specific value (here, the speed of a vehicle) can be displayed in a GUI

First, you need to run the client platform (client_example1.py) to obtain the access to the data

 ```python client_example1.py```
 
 Upon the request, enter your credentials: (username, password)
 
![](../../docs/images/example1_credentials.PNG)
 
 Once you have successfully registered, you can obtain a topic from which you can consume data.
 
![](../../docs/images/example1_python_cmd.png)
 
 Please to not close the platform client script.

 Then, open a new terminal and run the script example1_oscilloscope.py using the command prompted in the platform client
 
 ```python example1_oscilloscope.py topic platformaddress bootstrap_port registry_port```
 
 You will start receive data and extract speed value to show in live in a GUI
 
![](../../docs/images/speed_display.PNG)
![](../../docs/images/example1_speed_example.png)

## Example 2: Collection of location data with map display

In this example, we show how data can be collected from a given topic and location information is extracted and displayed using openstreetmap

First, you need to run the client platform (client_example2.py) to obtain the access to the data

 ```python client_example2.py```
 
 Upon the request, enter your credentials: (username, password)
 
![](../../docs/images/example2_credentials.PNG)
 
 Once you have successfully registered, you can obtain a topic from which you can consume data.
 
![](../../docs/images/example2_python_cmd.png)
 
 Please to not close the platform client script.

 Then, open a new terminal and run the script example2_displaymap.py using the command prompted in the platform client
 
 ```python example2_displaymap.py topic platformaddress bootstrap_port registry_port```
 
 You will start collecting a number of samples (e.g. 20) and store the location values in a dedicated table.
 At the end of the processing, a map file is generated to display the received location values. You can load the map file, 5GMETADataMap,html, in your browser to show the location values as illustrated in the figure below.
 
![](../../docs/images/display_browser.PNG)

## Example 3: Produce a feedback event from consumption of data

In this example, we show how a feedback event can be generated to a specific tile after the processing of data consumed from 5GMETA platform.

First, you need to run the client platform (client_example3.py) to obtain the access to the data

 ```python client_example3.py```
 
 Upon the request, enter your credentials: (username, password)
 
![](../../docs/images/example3_credentials.PNG)
 
 Once you have successfully registered, you will obtain the access to 2 topics: 1 for the consumption of data and 1 for the generation of an event feedback.
 
![](../../docs/images/example3_python_cmd.png)
 
 Please to not close the platform client script.

 Then, open a new terminal and run the script example3_eventfeedback.py using the command prompted in the platform client
 
 ```python example3_eventfeedback.py consume_topic produce_topic platformaddress bootstrap_port registry_port```
 
 You will start consuming data using the consume topic (e.g. 5). After the consumption of a predefined number of message, an event is produced on the 5GMETA platform as illustrated in the figure below.
 
![](../../docs/images/exemple3_data.PNG)
