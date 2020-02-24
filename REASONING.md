# Reasoning

This document describes the core ideas and components that can be used in order to reason about IoT incidents in Big Data applications.

[TOC]

* [High-Level Introduction](#high-level-introduction)
  * [Reference Big Data Pipeline](#reference-big-data-pipeline)
    + [Sensors](#sensors)
      - [Dataset](#dataset)
    + [MQTT Broker](#mqtt-broker)
    + [Apache Flink](#apache-flink)
    + [Apache NiFi](#apache-nifi)
    + [Node-RED](#node-red)
    + [Apache Hadoop](#apache-hadoop)
    + [Apache Spark](#apache-spark)
    + [Elasticsearch and Kibana](#elasticsearch-and-kibana)
    + [Minimum Deployment View (Docker)](#minimum-deployment-view--docker-)
  * [Concepts and Terminology](#concepts-and-terminology)
      - [Importance of abstraction](#importance-of-abstraction)
    + [Pipeline Component](#pipeline-component)
    + [Signal](#signal)
    + [Composite Signal](#composite-signal)
  * [Monitoring & Reasoning Pipeline](#monitoring---reasoning-pipeline)
    + [Simplified summary](#simplified-summary)
    + [Pipeline at a glance](#pipeline-at-a-glance)
      - [Note on modularity](#note-on-modularity)
    + [Log Collection: [Fluentd](https://www.fluentd.org/) ![Fluentd](https://avatars3.githubusercontent.com/u/859518?s=25&v=4)](#log-collection-fluentd-)
      - [How to collect logs](#how-to-collect-logs)
      - [The role of tags](#the-role-of-tags)
      - [How to capture signals](#how-to-capture-signals)
      - [Where to forward logs](#where-to-forward-logs)
    + [Metric Collection and Alerting: [Prometheus](https://prometheus.io/) ![Prometheus](https://avatars1.githubusercontent.com/u/3380462?s=25&v=4)](#metric-collection-and-alerting-prometheus-)
    + [Reliable Signal Collector: [Kafka](https://kafka.apache.org/) Pub/Sub <img src="https://images.safe.com/logos/formats/apache-kafka_100.png" alt="Kafka" width="25" />](#reliable-signal-collector-kafka-pubsub-)
    + [Prometheus->Kafka Bridge: Ingestion-Service by [Nest.js](https://nestjs.com/) ![Nestjs](https://avatars1.githubusercontent.com/u/28507035?s=25&v=4)](#prometheus-kafka-bridge-ingestion-service-by-nestjs-)
    + [Scalable Signal Reasoning: [Flink](https://flink.apache.org/) Reasoner Job <img src="https://sau.nobleprog.com/sites/hitrahr/files/category_images/height100_scale/apache_flink_training.png?t=f7fdbae1" alt="Flink" width="25" />](#scalable-signal-reasoning-flink-reasoner-job-)
    + [Dynamic Incident and Signal Knowledge Graph: [Neo4j](https://neo4j.com/) ![Neo4j](https://avatars1.githubusercontent.com/u/201120?s=25&v=4)](#dynamic-incident-and-signal-knowledge-graph-neo4j-)
    + [Long-term Storage of Recorded Signals: [Cassandra](https://cassandra.apache.org/) <img src="https://a.fsdn.com/allura/s/apache-cassandra/icon?1554403225?&amp;w=90" width="25" />](#long-term-storage-of-recorded-signals-cassandra-)
    + [Frequent-Pattern Mining: [Spark](https://spark.apache.org/) <img src="https://images.vogel.de/vogelonline/bdb/1596200/1596255/33.jpg" alt="Spark" width="25" />](#frequent-pattern-mining-spark-)
    + [Minimum Deployment View (Docker)](#minimum-deployment-view--docker--1)
  * [Limitations](#limitations)
  * [Potential Improvements](#potential-improvements)
      - [FP-Mining of Incidents](#fp-mining-of-incidents)
      - [Fluentd UI](#fluentd-ui)
      - [RDF Semantics + Neo4j](#rdf-semantics---neo4j)

## High-Level Introduction

Given an everyday Big Data pipeline responsible for ingesting, processing (both real-time and batch) and storing data of high volume, variety, velocity and veracity, so-called *incidents* (as defined in [TruongCompsac18](https://users.aalto.fi/~truongh4/publications/2018/truong-compsac2018-cr.pdf)) are inevitable. Individual incidents may happen on various abstraction layers such as physical (hardware issues), infrastructure (Cloud, VM issues), software (application platform and user application), and at the same time from the perspective of the data such as the lack of crucial information, high redundancy, inaccuracy, bad timeliness and many more. Obviously there are a lot of factors involved and usually individual incidents are connected to each other, either directly or indirectly in a cause-and-effect relationship. The bigger and more complex the pipeline gets, the ability to find the root-cause of incidents and to efficiently reason about them becomes exponentially more difficult.

In this document we provide a potential strategy with concrete tools and implementation for tackling this challenge. Goals include general applicability, extensibility, adaptability and scalability. Our proposed solution is based solely on open-source tools and focuses on the independence, modularity of specific tools s.t. these can be exchanged with others.

## Reference Big Data Pipeline

From now on *pipeline* refers to the examplery Big Data pipeline which requires monitoring and detection of incidents. The image below shows the structure of the pipeline and the flow of data.

![Big Data Pipeline Abstract Overview](https://raw.githubusercontent.com/rdsea/bigdataincidentanalytics/reasoning/documents/images/Pipeline%20Simple%20Overview.png)

Next, we will describe the function of each pipeline component. **Important note**: beware, that most of these components must be deployed in a cluster consisting of multiple containers. The image above does not reflect the deployment view, it is merely an abstraction.

### Sensors

The emission of sensor data is simulated by a simple Node.js application, which reads each row of a dataset and publishes it to the `customTopic` topic of the MQTT broker.

#### Dataset

The dataset is called *GNFUV Unmanned Surface Vehicles Sensor Data Set 2 Data Set* and is available for free on the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/GNFUV+Unmanned+Surface+Vehicles+Sensor+Data+Set+2). The data-set contains eight (2x4) data-sets of mobile sensor readings data (humidity, temperature) corresponding to a swarm of four Unmanned Surface Vehicles (USVs) in a test-bed, Athens, Greece.

The datasets have been adjusted so that only the attributes *device_id*, *humidity*, *temperature*, *time* are included. The simulated sensor sends records in a configurable time interval. The following shows a sample JSON encoded record that gets published to the MQTT broker:

```json
{
"device_id": "gnfuv-temp-exp1-55d487b85b-5g2xh",
"humidity": "21",
"temperature": "40",
"time": "2020-02-18T21:23:32.106Z"
}
```

For more information about implementation details, configuration and deployment please refer to the [Sensors Readme](pipeline/sensors/README.md).

### MQTT Broker

The MQTT broker is realized by a single [Mosquitto](https://mosquitto.org/) instance. It acts as a gateway between the sensor data coming from the oustide world and the other analytics components.

For more detailed information about configuration, deployment please refer to the [MQTT Broker Readme](pipeline/mqtt/README.md).

### Apache Flink

The [Apache Flink](https://flink.apache.org/) component represents a common stream-processing aggregation scenario. It consumes the sensor data from the MQTT broker, keys the stream by the `device_id` attritbute and calculates the averages of `humidity` and `temperature` for every 10 records. The resulting calculation is finally sent to the Elasticsearch component and stored under the `flink-sensor-data` index.

A sample JSON-encoded aggregation produced by Flink looks the following:

```json
{
    "id": "94778233-4024-4962-b398-dc179f144457",
    "deviceId": "gnfuv-temp-exp1-55d487b85b-5g2xh",
    "numberOfMeasurements": 10,
    "averageHumidity": 22,
    "averageTemperature": 39.5,
    "measurementIds": [
      "2020-01-15T10:59:09.549Z",
      "2020-01-15T10:59:10.615Z",
      "2020-01-15T10:59:11.618Z",
      "2020-01-15T10:59:12.620Z",
      "2020-01-15T10:59:13.623Z",
      "2020-01-15T10:59:14.626Z",
      "2020-01-15T10:59:15.631Z",
      "2020-01-15T10:59:16.633Z",
      "2020-01-15T10:59:17.637Z",
      "2020-01-15T10:59:18.641Z"
    ]
}
```

Note: the original dataset doesn't contain unique IDs for each recording. Since a report like this is in the scope of a single device, the timestamp of the record can be used as `measurementId`(more precisely, the pair of `deviceId` and `time` is always unique).

For more information about implementation details, configuration and deployment please refer to the [Flink Readme](pipeline/flink/README.md).

### Apache NiFi

The [Apache Nifi](https://nifi.apache.org/) component has the responsibility to simply consume the sensor records from the MQTT broker and storing them in a remote HDFS (= the Hadoop component). There is an already pre-built [NiFi Template](pipeline/nifi/IoT_Sensor_Data_Storage_Template.xml) in XML-format, which can be imported in the web-UI. The template containes the extended version showcasing how application-level signals can be emitted.

For more information about implementation details, configuration and deployment please refer to the [NiFi Readme](pipeline/nifi/README.md).

### Node-RED

The [Node-RED](https://nodered.org/) component represents a Node.js-based event-driven data pipeline. The provided *Flow* for our scenario simply consumes the sensor records from the MQTT broker and writes them to its local filesystem. The purpose of this component is to have technological heterogeneity with regard to platforms and programming languages. 

### Apache Hadoop

As introduced earlier, the Hadoop cluster merely acts as a storage component in our scenario. The sensor data collected in a HDFS is later leveraged by Spark to perform data analysis.

For more information about configuration and deployment please refer to the [Hadoop Readme](pipeline/hadoop/README.md).

### Apache Spark

The [Apache Spark](https://spark.apache.org/) component represents a large-scale data analytics use-case. The application reads sensor records from the remote HDFS and performs a Kmeans clustering on them based on the `humidity` and `temperature` attributes. The results are simply printed out. Again, for the purposes of heterogeneity, the application code is written in Python.

For more information about implementation details, configuration and deployment please refer to the [Spark Readme](pipeline/spark/README.md).

### Elasticsearch and Kibana

Apart from their usual responsibilities, these two components are present in order to close the Big Data analytics circle. [Elasticsearch](https://www.elastic.co/elasticsearch) is used to store Flink's generated reports and, as we will see later, logs and important events. [Kibana](https://www.elastic.co/kibana) is the visualizing component of the Delivery phase.

### Minimum Deployment View (Docker)

The [docker-compose.yml](docker-compose.yml) file contains all the definitions of services/containers that are required in order to run the reference pipeline. Only the Elasticsearch and Kibana services are not defined in this file, because they are also part of the monitoring pipeline (their definitions can be found in  [docker-compose.monitoring.yml](docker-compose.monitoring.yml). Please note that some of the services are meant to be scaled horizontally. Below  a diagram is shown, containing the minimum amount of containers.

![Reference Pipeline Deployment View](https://raw.githubusercontent.com/rdsea/bigdataincidentanalytics/reasoning/documents/images/Pipeline%20Deployment%20View.png)

## Concepts and Terminology

This section defines a set of terms that are relevant in the context of monitoring. 

#### Importance of abstraction

Monitoring of today's distributed systems involves a lot of complexity. In practice, no two systems are equal and given two companies the requirements usually differ greatly. As a consequence there is no silver bullet or an off-the-shelf solution suitable for all systems. This is especially true for Big Data applications. In order to avoid chaos and gain observability into these applications, it is important to keep up a certain level of abstraction. 

When it comes to the reasoning about which set of event(s) led to a certain outcome, it doesn't play a crucial role, whether the system or parts of the system are self-hosted or deployed in the Cloud, as long as we are able to gather the relevant information about a certain symptom leading to an incident. Consider the following example: the Flink component indicates that there are no records coming from the MQTT Broker. This is an actual symptom that we ought to be made aware of, because the constant processing of sensor data is crucial for meeting SLAs and SLOs. The cause, in our case the MQTT broker not delivering data, can potentially be one of myriad issues. From the reasoning perspective, it doesn't necessarily matter how the broker has been deployed. Maybe the broker instance has been restarted; maybe there is a temporary network outage; maybe the Cloud environment does its job at being elastic and redistributes the load. The important thing here is the ability to capture the issue at Flink and clearly pointing the cause to the MQTT broker. 

### Pipeline Component

As you will see, the term *pipeline component* gets used a lot in the monitoring/reasoning parts. In order to maintain abstraction, this term refers to a component which is part of the whole Big Data pipeline and has a clear function. For example, in the very first diagram Flink is a pipeline component, while the user-defined job, the jobmanager and task managers are simply parts of that component. 

### Signal

During monitoring of Big Data pipelines, we need to deal with a great amount of log data, metrics, traces, all in different formats, shapes and semantics. In order to cut through the noise and abstract away any potential (future) ways of expressing monitoring data, we introduce the notion of a *Signal*. 

As defined in the book [Introductory Signal Processing](https://books.google.at/books?id=QBT7nP7zTLgC&pg=PA1&redir_esc=y#v=onepage&q&f=false), a signal "*is a function that conveys information about a phenomenon.*" In this context, a signal can be any sort of data, message that expresses an important event used later for incident reasoning. As we will see later, in the scope of this project there are 3 ways to generate signals, all of them in a JSON format:

* marking of infrastrucure/platform logs as signals
* emitting user-written signals from the application level
* construct Prometheus alerts on any level which will be interpreted as signals

The Monitoring & Reasoning Pipeline section goes into more detail how a Signal should look like.

### Composite Signal

A Compsite Signal is the same as a simple Signal as far as its intentions are concerned. The difference is however, that a Composite Signal is composed of one or more Signals. This concept makes it easily possible to combine arbitrary Signals which indicate incidents.

## Monitoring & Reasoning Pipeline

This section describes the proposed monitoring solution in detail including the selection of tools and required instrumentation steps.

### Simplified summary

Ignoring the specific chosen tools, the whole process can be summarized into the following steps:

* centralize telemetry data as best as possible
  * structured logs: infrastructure, platform, user-application
  * metrics in a time-series format
* annotate, mark any telemetry data that are signalling important events, incidents so that these are handled as *Signals* instead of *noise* 
* the Reasoner processes each Signal one by one in real-time; with the help of the underlying Knowledge Graph it determines whether the Signal is fired, and whether it indicates any incidents
  * if an incident gets activated, a report is generated and sent to the visualizing component
  * additionally, each Signal's occurrence is recorded for future analysis
* perform on-demand Frequent-Pattern Mining on the historical Signal records, discover correlated Signals and provide recommendations for combining multiple Signals into CompositeSignals
* maintain the Knowledge Graph by adding new CompositeSignals and indicated incidents

### Pipeline at a glance

The image below illustrates the monitoring/reasoning pipeline. In the following, we will describe each component's purpose and requirements in detail.

![Monitoring Pipeline](https://raw.githubusercontent.com/rdsea/bigdataincidentanalytics/reasoning/documents/images/Monitoring%20Pipeline%20Overview.png)

#### Note on modularity

While the pipeline may seem unreasonably fragmented, this is not a side effect. As stated earlier, an important goal is to uphold a high degree of independence when it comes to tooling, because most companies already have some sort of monitoring procedures in place. With this architecture and minimal requirements, it should be fairly easy to swap out specific tools with preferred ones fulfilling the same purposes. In each component's description below, we will provide a reason for picking the tool as well as other alternatives.

### Log Collection: [Fluentd](https://www.fluentd.org/) ![Fluentd](https://avatars3.githubusercontent.com/u/859518?s=25&v=4)

**Goal**: centralize logs of each participating component and of each layer in a structured, JSON-format.

**Why** Fluentd: it is free, open-source and part of the [Cloud Native Computing Foundation](https://www.cncf.io/projects/) (CNCF) with a graduated status. It has extensive lists of plugins for both input and output sources, and can be easily customized.

**Alternative(s)**: Logstash, Splunk, DataDog, Greylog

#### How to collect logs

How log data gets into Fluentd very much depends on the amount of control one has over the target component:

* **No control**, only a Docker container or rolling log files: we make the assumption that we either have access to the Docker container or the output log files of the target component. In the latter case, the small sibling of Fluentd, called [Fluent Bit](http://fluentbit.org/) is recommended to be deployed as a sidecar container which tails the log file of the component and forwards the logs to the central Fluentd instance. Alternatively, for any given Docker container it is possible set a remote Fluentd instance as the log driver ([official documentation](https://docs.docker.com/config/containers/logging/fluentd/)). In the prototype [docker-compose.yml](docker-compose.yml) the definition looks like this:

  ```yaml
  logging:
  	driver: fluentd
    options:
    	fluentd-address: localhost:24224
      tag: com.rdsea.sensor.{{.ID}}.platform
      fluentd-sub-second-precision: "true"
  ```

  The `tag` is an essential part of the log collection in Fluentd, because further processing, filtering of records are based on the parts of a tag.

* **Control over the platform**, e.g. Flink, NiFi, Node-RED: for many languages and tools, it is possible to integrate a dependency that will take care of forwarding the logs to Fluentd. For example, in case of tools which support the configuration of a `logback.xml` file, an external JAR dependency called [Fluency](https://github.com/komamitsu/fluency) should be packaged along. In the Logback configuration, the additional appender can be specified as shown below. For more details, please refer to the respective components' README:

  ```xml
  <appender name="FLUENCY_SYNC" class="ch.qos.logback.more.appenders.FluencyLogbackAppender">
    <!-- The tag each log record will get. Here, the tag value is externalized as an environment variable. -->
          <tag>${LOG_COMPONENT_TAG}</tag>
  
          <!-- Host name/address and port number which Flentd placed -->
          <remoteHost>fluentd</remoteHost>
          <port>24224</port>
  
          <!-- [Optional] Additional fields(Pairs of key: value) -->
          <!--<additionalField>
              <key>key</key>
              <value>value</value>
          </additionalField>-->
  
          <flushIntervalMillis>1200</flushIntervalMillis>
          <useEventTime>true</useEventTime>
          <sslEnabled>false</sslEnabled>
          <flattenMapMarker>false</flattenMapMarker>
  
          <encoder>
              <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
          </encoder>
      </appender>
  ```

  For NPM-based applications, such as Node-RED there is a [fluent-logger](https://www.npmjs.com/package/fluent-logger) NPM package which can be leveraged to output any platform logs to a remote Fluentd instance.

* **Control over the user-application level**: depending on the specific application there may be some limitations, but for the majority of the cases it is recommended to use one of the Fluentd SDKs. There are libraries available for all major programming languages. Please refer to the pipeline components on how to use the libraries.

#### The role of tags

tbd

#### How to capture signals

tbd

* for concrete, user-defined signals on the application-layer (e.g. incomplete data detected), the `signal.<SIGNAL_NAME>` part should be included in the tag when emitting the log. This way, the log will be immediately forwarded to Kafka
* for logs which are emitted by the platform as-is, special regex-rules should be defined. For example,if we find a specific platform log to be of significance, we can capture it using a regex pattern and replace its tag with `signal.<SIGNAL_NAME>`.

#### Where to forward logs

There is a wide selection of output plugins, so that targets can be tailored to requirements. In our monitoring pipeline, as illustrated in the architecture image, logs are separated into two subsets, *Signal*s and *Noise*s. Every log that receives the annotation `signal` in its tag is interpreted as a *Signal* and therefore will be broadcast to the Kafka component's `signals` topic. The rest of the logs are pushed to Elasticsearch, where DevOps and stakeholders can further use/visualize the data for other purposes.



### Metric Collection and Alerting: [Prometheus](https://prometheus.io/) ![Prometheus](https://avatars1.githubusercontent.com/u/3380462?s=25&v=4)

tbd

### Reliable Signal Collector: [Kafka](https://kafka.apache.org/) Pub/Sub <img src="https://images.safe.com/logos/formats/apache-kafka_100.png" alt="Kafka" width="25" />

tbd

### Prometheus->Kafka Bridge: Ingestion-Service by [Nest.js](https://nestjs.com/) ![Nestjs](https://avatars1.githubusercontent.com/u/28507035?s=25&v=4)

tbd

### Scalable Signal Reasoning: [Flink](https://flink.apache.org/) Reasoner Job <img src="https://sau.nobleprog.com/sites/hitrahr/files/category_images/height100_scale/apache_flink_training.png?t=f7fdbae1" alt="Flink" width="25" />

tbd

### Dynamic Incident and Signal Knowledge Graph: [Neo4j](https://neo4j.com/) ![Neo4j](https://avatars1.githubusercontent.com/u/201120?s=25&v=4)

tbd

### Long-term Storage of Recorded Signals: [Cassandra](https://cassandra.apache.org/) <img src="https://a.fsdn.com/allura/s/apache-cassandra/icon?1554403225?&amp;w=90" width="25" />

tbd

### Frequent-Pattern Mining: [Spark](https://spark.apache.org/) <img src="https://images.vogel.de/vogelonline/bdb/1596200/1596255/33.jpg" alt="Spark" width="25" />

tbd

### Minimum Deployment View (Docker)

## Limitations

CompositeSignals and incidents need to be manually added to the Knowledge Graph, currently using the Neo4j Browser. Although thanks to the Cypher language this is not a difficult task, there should be some kind of interface with better usability which also hides Neo4j as the used database. 

The synchronization of clocks and handling of time-zones is out of scope of this project. For every component UTC timestamps are assumed. Furthermore, Signals in the reasoner are processed in the order they arrive. We rely on Kafka's correct ordering. 

## Potential Improvements

#### FP-Mining of Incidents

Frequent Pattern Mining could be applied to incidents as well. The idea is that every inferred incident should be recorded (just like the Signals at the moment) and find correlating, not yet connected incidents through FP-mining. Example: if, let's say, in 100% of the time *Incident_B* happens after some time of *Incident_A*, then it may be likely that these two are not just correlated, but also reveal causation.

#### Fluentd UI

[Fluentd UI](https://docs.fluentd.org/deployment/fluentd-ui) offers the capability to manage running Fluentd instances through the web browser. Since a good chunk of annotations have to be carried out in the configuration files, Fluentd UI would make this task much more convenient. Unfortunately it showed some issues in testing, therefore this remains as a future improvement for the time being.

#### RDF Semantics + Neo4j

There has been rudimentary experiments with RDF-based semantics and Neo4j by [Neo4j Labs](https://github.com/neo4j-labs/neosemantics). Approaches like this could be investigated to improve the inferencing part of the monitoring.



