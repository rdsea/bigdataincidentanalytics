# Monitoring and Detection of Incidents in Big Data Applications 

This project illustrates monitoring, detection and classification of incidents in Big Data applications on a simple example. 

## Introduction

We introduce a generic architecture of an incident management system that can be adapted to the specific needs of Cloud-based Big Data analytics applications. 

![alt text](https://github.com/rdsea/bigdataincidentanalytics/blob/master/documents/images/MonitoringImplementation.png)

Following components are included in the prototype: 
* Generic classification database
* Monitoring agent 
* Detection and classification rule

## Installation 

### Required Software

The simulation of the Big Data application uses the following Software to simulate the incident used to illustrate the functionality: 

* Elasticserach
* Logstash 
* Kibana
* Docker
* Docker compose
* https://github.com/rdsea/IoTCloudSamples
* Hadoop
* Apache Nifi 
* Neo4j 
* Python
* Pythonenv

### Run the identification and classification rule 

A short overview of the scenario. 

![alt text](https://github.com/rdsea/bigdataincidentanalytics/blob/master/documents/images/SimplifiedMotivatingScenario.png)

1. Install Docker and Docker Compose
2. Load the project from https://github.com/rdsea/IoTCloudSamples
https://github.com/rdsea/IoTCloudSamples. This is used to simulate the IoT sensors producing data. 
3. This scenario starts different Docker containers for further information refer to the product documentation pipenv run python provision.py config.sample.yml
4. Install hadoop and start the hdfs
5. Start Apache Nifi and configure
  * Configure a MQTT processer consuming the MQTT stream from the Dockerised components
  * Configure a HDFS processor writing into the local hdfs 
  * Configure a message queue between the two processors

6. Install Elasticsearch 
7. Install Kibana
8. Install Neo4j 
  * The database has to be initialised with the classification 

9. Install logstash on the server running the Apache Nifi installation 
10. Configure logstash with nifiPipeline.conf
11. Install python on the server running the rules 
12. Execute the nifirule.py

The log data from the test scenario is fetched and fed via logstash into elasticsearch. The rule then identifies, classifies and writes the incident into elasticsearch where it can be visualised with Kibana. 
 
