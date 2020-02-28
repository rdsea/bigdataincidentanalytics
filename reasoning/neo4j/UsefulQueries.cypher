//MATCH (n) DETACH DELETE n
match (n) return n

//match (n:DataPipeline)-[r]-(m) return n,r,m
CREATE (s:Element {name:'Signal'})

match (s:Element {name:'Signal'})
CREATE (l:Signal {name:'Log'})
CREATE (a:Signal {name:'Alert'})
CREATE (s)-[:IS]->(l),(s)-[:IS]->(a)

MATCH (a:Signal {name:'Alert'})
CREATE (p:Alert {name:'PrometheusAlert'}), (a)-[:IS]->(p)

MATCH (l:Signal {name:'Log'})
MATCH (broker:DataPipeline {name:'MQTT_BROKER'})
CREATE (s:Element {name:'CompositeSignal'})
CREATE (cs:CompositeSignal {name:'Cs1'})
CREATE (m:Log {name:'MqttShutdownLog'}), (l)-[:IS]->(m),(m)-[:SIGNALLED_BY]->(broker),(m)-[:PART_OF]->(cs),(s)-[:IS]->(cs)

MATCH (n:Element {name:'Incident'})
MATCH (cs:CompositeSignal {name:'Cs1'})
CREATE (i:Incident {name:'DataLoadingIncident1'}), (n)-[:IS]->(i),(cs)-[:INDICATES]->(i)

// Adding a new signal. The DataPipeline component can be extracted from the signal
MATCH (p:DataPipeline {name:'MQTT_BROKER'})
MATCH (cs:CompositeSignal {name:'Cs1'})
CREATE (s:Signal {name:'MqttStartupLog'})
CREATE (s)-[:SIGNALLED_BY]->(p), (s)-[:PART_OF]->(cs)

// Get the incidents a given CompositeSignal indicates, collected into a list.
// In this example we query for the indicated incidents of the 'MqttUnavailable' CompositeSignal
MATCH (cs:CompositeSignal {name:'MqttUnavailable'})
MATCH (cs)-[:INDICATES]->(incident:Incident)
  WHERE duration.inSeconds(COALESCE(r.lastActivation,localdatetime('1990-01-01T00:00:00')),localdatetime()).seconds > 120
SET r.lastActivation=localdatetime()
RETURN collect(incident)

// Update the activationTime relationship property on each edge between this signal and its corresponding CompositeSignals
// + Retrieve all CompositeSignals which contain this signal and have at least as many activated signals as their threshold specifies
MATCH (s:Signal {name:'MqttStartupLog'})
MATCH (comp:DataPipeline {name:'MQTT_BROKER'})
MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))
FOREACH (r IN relationships(rels) | SET r.activationTime=datetime('2020-02-08T18:23:19.810000000') )
WITH cs
MATCH ()-[signalRels:PART_OF]->(cs)
WITH cs, count(signalRels) as numConnectedSignal
MATCH (n)-[activeSignalRels:PART_OF]->(cs), (n)-[:SIGNALLED_BY]->(comp:DataPipeline)
  WHERE EXISTS (activeSignalRels.activationTime)
WITH cs, n as activatedSignal, numConnectedSignal, activeSignalRels.activationTime as actTime, comp.name as componentName
MATCH ()-[r:PART_OF]->(cs)
  WHERE EXISTS (r.activationTime)
WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, count(r) as numActiveComponents
MATCH (cs)
WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, numActiveComponents
  WHERE toInteger(ceil(cs.activationThreshold * numConnectedSignal)) <= numActiveComponents
RETURN cs {.*, activeSignals: collect(activatedSignal {.*, timestamp: actTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignal}


// Full-fledged query for when a Signal fires and we want to collect every CompositeSignal with enough activated Signals
// This query uses an example Signal with name 'NotReceivingSensorData', pipeline component 'NODE-RED' and a timestamp "2020-02-20T12:58:55.835526300"
MATCH (dp:DataPipeline{name:'NODE-RED'})
MATCH (e:Element{name:'Signal'})
MERGE (s:Signal {name:'NotReceivingSensorData'})
MERGE (s)-[:SIGNALLED_BY]->(dp)
MERGE (e)-[:IS]->(s)
SET s.thresholdCounter = null
SET s.lastSignalTime = localdatetime("2020-02-20T12:58:55.835526300")
SET s.summary = "Some summary"
SET s.details = "Some detail"
WITH s
MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))
FOREACH (r IN relationships(rels) | SET r.activationTime=localdatetime("2020-02-20T12:58:55.835526300") )
WITH cs
MATCH ()-[signalRels:PART_OF]->(cs)
WITH cs, count(signalRels) as numConnectedSignal
MATCH (n)-[activeSignalRels:PART_OF]->(cs), (n)-[:SIGNALLED_BY]->(comp:DataPipeline)
  WHERE EXISTS (activeSignalRels.activationTime)
WITH cs, n as activatedSignal, numConnectedSignal, activeSignalRels.activationTime as actTime, comp.name as componentName
MATCH ()-[r:PART_OF]->(cs)
  WHERE EXISTS (r.activationTime)
WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, count(r) as numActiveComponents
MATCH (cs)
WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, numActiveComponents
  WHERE toInteger(ceil(COALESCE(cs.activationThreshold,1.0) * numConnectedSignal)) <= numActiveComponents
RETURN cs {.*, activeSignals: collect(activatedSignal {.*, timestamp: actTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignal}


// Query for the Frequent Pattern Mining part
// Given a list of signals, the query returns all the CompositeSignals that this list is part of
// Consequently, if the query returns an empty list it means that according to the FP-algorithm the
// input list should be made into a CompositeSignal
WITH ['MqttStartupLog','MqttShutdownLog','MqttdownAlert'] as signalNames, ['MQTT_BROKER','NIFI','MQTT_BROKER'] as signalComponents
UNWIND range(0,size(signalNames)-1) as i
MATCH (s:Signal)-[:PART_OF]->(cs:CompositeSignal)
MATCH (s:Signal)-[:SIGNALLED_BY]->(pc:DataPipeline)
  WHERE s.name=signalNames[i] AND pc.name=signalComponents[i]
WITH s, collect(cs) as compositeSignalPerSignal
WITH collect(compositeSignalPerSignal) as compositeSignals
WITH reduce(commonCompositeSignals = head(compositeSignals), compSig in tail(compositeSignals)|
     apoc.coll.intersection(commonCompositeSignals,compSig)) as commonCompositeSignals
RETURN commonCompositeSignals

// Delete node by ID, plus delete relationships it is connected to
MATCH (dp:DataPipeline) WHERE ID(dp)=120
OPTIONAL MATCH (dp)-[r]-()
DELETE r, dp

MATCH (dp:DataPipeline{name:'NODE-RED'})
MATCH (e:Element{name:'Signal'})
MERGE (s:Signal {name:'NotReceivingSensorData'})
MERGE (s)-[:SIGNALLED_BY]->(dp)
MERGE (e)-[:IS]->(s)
RETURN s

// Insert a new incident as well as a new Composite Signal.
MATCH (e:Element {name:'Incident'})
MATCH (cs:Element {name:'CompositeSignal'})
CREATE (compSig:CompositeSignal {name:'MqttUnavailable'}) // this is the new CompositeSignal node
CREATE (i:Incident {name:'Data Loading Incident'}) // this is the new Incident node
CREATE (e)-[:IS]->(i), (cs)-[:IS]->(compSig),(compSig)-[:INDICATES]->(i) // here we connect the nodes via relationships
RETURN compSig // optionally return, but not needed

// Connect an existing Signal to an existing Composite Signal via the PART_OF relationship
MATCH (signal:Signal {name:'NotReceivingSensorData'})
MATCH (compositeSignal:CompositeSignal {name:'MqttUnavailable'})
CREATE (signal)-[:PART_OF]->(compositeSignal)

// Insert a new Signal emitted by NIFI. This connection error Signal requires 3 occurrences within 60 seconds
MATCH (dp:DataPipeline{name:'NIFI'})
MATCH (e:Element{name:'Signal'})
MERGE (s:Signal {name:'MqttConnectionError'})
MERGE (s)-[:SIGNALLED_BY]->(dp)
MERGE (e)-[:IS]->(s)
SET s.threshold=3
SET s.coolDownSec=60;

// With this single query, we can combine multiple Signals into a new (or existing) CompositeSignal.
// If the CompositeSignal with the given name doesn't exist yet, it will be created.
// If the Incident with the given name doesn't exist yet, it will be created.
// The last 2 "SET" lines are optional; with these you can set/update the properties of the CompositeSignal
WITH ['NotReceivingSensorData','MqttConnectionError','MqttInstanceDownAlert'] as signalNames,
     ['NODE-RED','NIFI','MQTT_BROKER'] as signalComponents
UNWIND range(0,size(signalNames)-1) as i
MATCH (s:Signal)-[:SIGNALLED_BY]->(pc:DataPipeline)
  WHERE s.name=signalNames[i] AND pc.name=signalComponents[i]
MATCH (cs:Element {name:'CompositeSignal'})
MATCH (ie:Element {name:'Incident'})
MERGE (compSig:CompositeSignal {name:'MqttUnavailable'})
MERGE (cs)-[:IS]->(compSig)
MERGE (incident:Incident {name:'Data Loading Incident'})
MERGE (ie)-[:IS]->(incident)
MERGE (compSig)-[:INDICATES]->(incident)
MERGE (s)-[:PART_OF]->(compSig)
SET compSig.activationThreshold=0.5 // this is optional, will fall back to 1.0 as default if not exists
SET compSig.coolDownSec=30; // // this is optional, will fall back to 60 as default if not exists

// Example: Delete a wrongly named/created CompositeSignal. This query first deletes all the relationships the node
// is connected to before deleting the node itself.
MATCH (cs:CompositeSignal {name:'2MqttUnavailable'})-[r]-()
DELETE r, cs;

// Reset the activationTime relationship property of between a given list of Signals and a CompositeSignal
WITH ['MqttConnectionError','MqttConnectionFailureLog'] as signalNames,
     ['NIFI','NODE-RED'] as signalComponents
UNWIND range(0,size(signalNames)-1) as i
MATCH (Element {name:'CompositeSignal'})-[:IS]->(cs:CompositeSignal {name:'MqttUnavailable'})
MATCH (s:Signal)-[:SIGNALLED_BY]->(pc:DataPipeline)
  WHERE s.name=signalNames[i] AND pc.name=signalComponents[i]
MATCH (s)-[r:PART_OF]->(cs)
SET r.activationTime=null;