CREATE (Ownership:Element{name:'Ownership'})
CREATE (TotalOwnership:Ownership{name:'Total ownership'})
CREATE (PartialOwnership:Ownership{name:'Partial ownership'})
CREATE (NoOwnership:Ownership{name:'None'})
CREATE (AOwnership:PartialOwnership{name:'App'})
CREATE (POwnership:PartialOwnership{name:'Plt'})
CREATE (IOwnership:PartialOwnership{name:'Inf'})
CREATE (APOwnership:PartialOwnership{name:'App + Plt'})
CREATE (AIOwnership:PartialOwnership{name:'App + Inf'})
CREATE (PIOwnership:PartialOwnership{name:'Plt + Inf'})
CREATE (Layer:Element{name:'Layer'})
CREATE (Application:Layer{name:'Application'})
CREATE (Platform:Layer{name:'Platform'})
CREATE (Infrastructure:Layer{name:'Infrastructure'})
CREATE (Incident:Element{name:'Incident'})
CREATE (DataIncident:Incident{name:'Data incident'})
CREATE (ConnectionIncident:Incident{name:'Connection incident'})
CREATE (MemoryIncident:Incident{name:'Memory incident'})
CREATE (MonitoringInformation:Element{name: 'Monitoring Information'})
CREATE (InstrumentedLogs:MonitoringInformation{name: 'Instrumented Logs'})
CREATE (DockerLogs:MonitoringInformation{name: 'Docker Logs'})
CREATE (Metrics:MonitoringInformation{name: 'Metrics'})

CREATE (PipeLineComp:Element{name:'Pipeline Component'})
CREATE (Sensor:PipeLineComp{name:'Sensor'})
CREATE (MQTT:PipeLineComp{name:'MQTT'})
CREATE (Nifi:PipeLineComp{name:'Apache Nifi'})
CREATE (Flink:PipeLineComp{name:'Apache Flink'})
CREATE (Node_RED:PipeLineComp{name:'Node-RED'})
CREATE (HDFS:PipeLineComp{name:'HDFS'})

CREATE (Ownership)-[:HAS_DEGREE]->(TotalOwnership),
       (Ownership)-[:HAS_DEGREE]->(PartialOwnership),
       (Ownership)-[:HAS_DEGREE]->(NoOwnership),
       (PartialOwnership)-[:CAN_BE]->(AOwnership),
       (PartialOwnership)-[:CAN_BE]->(POwnership),
       (PartialOwnership)-[:CAN_BE]->(IOwnership),
       (PartialOwnership)-[:CAN_BE]->(APOwnership),
       (PartialOwnership)-[:CAN_BE]->(AIOwnership),
       (PartialOwnership)-[:CAN_BE]->(PIOwnership),
       (Layer)-[:IS]->(Application),
       (Layer)-[:IS]->(Platform),
       (Layer)-[:IS]->(Infrastructure),
       (Application)-[:PROVIDES]->(InstrumentedLogs),
       (Platform)-[:PROVIDES]->(DockerLogs),
       (Infrastructure)-[:PROVIDES]->(Metrics),
       (Incident)-[:IS]->(DataIncident),
       (Incident)-[:IS]->(ConnectionIncident),
       (Incident)-[:IS]->(MemoryIncident),
       (MonitoringInformation)-[:IS]->(InstrumentedLogs),
       (MonitoringInformation)-[:IS]->(DockerLogs),
       (MonitoringInformation)-[:IS]->(Metrics),
       (TotalOwnership)-[:CONTROLS]->(Application),
       (TotalOwnership)-[:CONTROLS]->(Platform),
       (TotalOwnership)-[:CONTROLS]->(Infrastructure),
       (AOwnership)-[:CONTROLS]->(Application),
       (POwnership)-[:CONTROLS]->(Platform),
       (IOwnership)-[:CONTROLS]->(Infrastructure),
       (APOwnership)-[:CONTROLS]->(Application),
       (APOwnership)-[:CONTROLS]->(Platform),
       (AIOwnership)-[:CONTROLS]->(Application),
       (AIOwnership)-[:CONTROLS]->(Infrastructure),
       (PIOwnership)-[:CONTROLS]->(Platform),
       (PIOwnership)-[:CONTROLS]->(Infrastructure),
       (InstrumentedLogs)-[:ABLE_TO_CAPTURE]->(DataIncident),
       (DockerLogs)-[:ABLE_TO_CAPTURE]->(ConnectionIncident),
       (Metrics)-[:ABLE_TO_CAPTURE]->(MemoryIncident),
       (PipeLineComp)-[:HAS]->(Ownership),
       (PipeLineComp)-[:IS]->(MQTT),
       (PipeLineComp)-[:IS]->(Nifi),
       (PipeLineComp)-[:IS]->(Flink),
       (PipeLineComp)-[:IS]->(Node_RED),
       (PipeLineComp)-[:IS]->(HDFS),
       (PipeLineComp)-[:IS]->(Sensor)
;