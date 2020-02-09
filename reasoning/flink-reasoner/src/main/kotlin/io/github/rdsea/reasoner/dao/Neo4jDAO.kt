package io.github.rdsea.reasoner.dao

import com.google.gson.Gson
import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.domain.IncidentEntity
import io.github.rdsea.reasoner.domain.Signal
import java.io.Serializable
import java.util.Optional
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values.parameters
import org.neo4j.driver.types.Node
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class Neo4jDAO : DAO, Serializable {

    private lateinit var driver: Driver
    private lateinit var log: Logger
    private lateinit var gson: Gson

    override fun initialize() {
        driver = Main.getNeo4jDriver()
        log = LoggerFactory.getLogger(Neo4jDAO::class.java)
        gson = Main.gson
    }

    override fun findSignal(signal: Signal): Optional<Signal> {
        driver.session().use { session ->
            val result: Result = session.run(
                FIND_SIGNAL_QUERY,
                parameters("name", signal.name, "component", signal.pipelineComponent)
            )
            if (result.hasNext()) {
                val node: Node = result.next().get("s").asNode()
                val persistedSignal = parseNeoSignalNode(node)
                return Optional.of(mergeSignals(signal, persistedSignal))
            }
            return Optional.empty()
        }
    }

    override fun updateSignalAndGetActivatedCompositeSignals(signal: Signal): List<CompositeSignal> {
        driver.session().use { session ->
            return session.writeTransaction { tx ->
                val res = tx.run(
                    if (signal.isActivated()) UPDATE_SIGNAL_ACTIVATED_QUERY else UPDATE_SIGNAL_SIMPLE_QUERY,
                    parameters(
                        "name", signal.name, "component", signal.pipelineComponent,
                        "counter", if (signal.thresholdCounter == -1) null else { signal.thresholdCounter },
                        "time", signal.timestamp,
                        "summary", signal.summary, "details", gson.toJson(signal.details)
                    )
                )
                if (!signal.isActivated()) {
                    emptyList()
                } else {
                    val compositeSignals = mutableListOf<CompositeSignal>()
                    while (res.hasNext()) {
                        val record: Record = res.next()
                        val cs = gson.fromJson(gson.toJsonTree(record["cs"].asMap()), CompositeSignal::class.java)
                        log.info("parsed composite signal: $cs")
                        compositeSignals.add(cs)
                    }
                    compositeSignals
                }
            }
        }
    }

    override fun findCompositeSignalsOfSignal(signal: Signal): List<CompositeSignal> {
        driver.session().use { session ->
            val result: Result = session.run(
                FIND_CS_OF_SIGNAL_QUERY,
                parameters("name", signal.name, "component", signal.pipelineComponent)
            )
            val signals = mutableListOf<CompositeSignal>()
            while (result.hasNext()) {
                val record: Record = result.next()
                signals.add(parseNeoRecord(record))
            }
            return signals
        }
    }

    override fun updateCompositeSignal(compositeSignal: CompositeSignal) {
        /*driver.session().use { session ->
            session.writeTransaction { tx ->
                tx.run(
                    UPDATE_COMP_SIGNAL_QUERY,
                    parameters("x", compositeSignal.name, "y")
                )
            }
        }*/
        throw UnsupportedOperationException()
    }

    override fun findIncidentsOfCompositeSignal(compositeSignal: CompositeSignal): List<IncidentEntity> {
        driver.session().use { session ->
            val result: Result = session.run(INCIDENTS_AND_SIGNALS_QUERY, parameters("name", compositeSignal.name))
            val incidents = mutableListOf<Incident>()
            /*while (result.hasNext()) {
                val record: Record = result.next()
                val incidentNode = record["incident"].asNode()
                val signalsNodeList = record["collect(sig)"].asList { it.asNode() }
                val signalsList = signalsNodeList.map { parseNeoSignalNode(it) }
                incidents.add(Incident(incidentNode.get("name").asString(), compositeSignal.lastSignalTime!!, compositeSignal, signalsList))
            }*/
            if (result.hasNext()) {
                val record: Record = result.next()
                return record["collect(incident)"].asList { it.asMap() }.map { gson.fromJson(gson.toJsonTree(it), IncidentEntity::class.java) }
            }
            return emptyList()
        }
    }

    override fun tearDown() {
        driver.close()
    }

    private fun parseNeoRecord(record: Record): CompositeSignal {
        val numOfConnectedSignals = record["cnt"].asInt()
        val node = record.get("n").asNode()
        val res = gson.fromJson(gson.toJsonTree(node.asMap()), CompositeSignal::class.java)
        res.numOfConnectedSignals = numOfConnectedSignals
        return res
    }

    private fun parseNeoSignalNode(node: Node): Signal {
        return gson.fromJson(gson.toJson(node.asMap()), Signal::class.java)
    }

    private fun mergeSignals(incoming: Signal, persisted: Signal): Signal {
        return Signal(
            type = incoming.type,
            name = incoming.name,
            timestamp = incoming.timestamp,
            pipelineComponent = incoming.pipelineComponent,
            summary = incoming.summary,
            details = incoming.details,
            threshold = persisted.threshold,
            thresholdCounter = persisted.thresholdCounter,
            lastSignalTime = persisted.lastSignalTime,
            coolDownSec = persisted.coolDownSec
        )
    }

    companion object {
        private const val serialVersionUID = 20180617104400L
        private const val MATCH_SIGNAL_BY_COMP = "MATCH (s:Signal {name:\$name}) " +
            "MATCH (s)-[:SIGNALLED_BY]->(:DataPipeline {name:\$component}) "
        private const val FIND_SIGNAL_QUERY = MATCH_SIGNAL_BY_COMP + "RETURN s"
        private const val UPDATE_SIGNAL_SIMPLE_QUERY = MATCH_SIGNAL_BY_COMP +
            "SET s.thresholdCounter = \$counter " +
            "SET s.lastSignalTime = \$time " +
            "SET s.summary = \$summary " +
            "SET s.details = \$details "
        private const val FIND_CS_OF_SIGNAL_QUERY = MATCH_SIGNAL_BY_COMP +
            "MATCH (s)-[:PART_OF]->(n:CompositeSignal) " +
            "MATCH ()-[r:PART_OF]->(n:CompositeSignal) " +
            "WITH n,count(r) as cnt " +
            "RETURN n,cnt"
        private const val UPDATE_COMP_SIGNAL_QUERY = "MATCH (s:CompositeSignal {name:\$x}) " +
            "SET s.lastSignalTime = \$y " +
            "SET s.activeSignals = \$z"
        private const val INCIDENTS_AND_SIGNALS_QUERY = "MATCH (cs:CompositeSignal {name:\$name})\n" +
            "MATCH (cs)-[:INDICATES]->(incident:Incident)\n" +
            "RETURN collect(incident)"
        // "UNWIND cs.activeSignals as s\n" +
        // "MATCH (sig:Signal {name:s})\n" +
        // "RETURN incident,collect(sig)"
        private const val UPDATE_SIGNAL_ACTIVATED_QUERY = UPDATE_SIGNAL_SIMPLE_QUERY +
            "WITH s\n" +
            "MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))\n" +
            "FOREACH (r IN relationships(rels) | SET r.activationTime=\$time )\n" +
            "WITH cs\n" +
            "MATCH ()-[signalRels:PART_OF]->(cs)\n" +
            "WITH cs, count(signalRels) as numConnectedSignal\n" +
            "MATCH (n)-[activeSignalRels:PART_OF]->(cs), (n)-[:SIGNALLED_BY]->(comp:DataPipeline)\n" +
            "WHERE EXISTS (activeSignalRels.activationTime)\n" +
            "WITH cs, n as activatedSignal, numConnectedSignal, activeSignalRels.activationTime as actTime, comp.name as componentName\n" +
            "MATCH ()-[r:PART_OF]->(cs)\n" +
            "WHERE EXISTS (r.activationTime)\n" +
            "WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, count(r) as numActiveComponents\n" +
            "MATCH (cs)\n" +
            "WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, numActiveComponents\n" +
            "WHERE toInteger(ceil(cs.activationThreshold * numConnectedSignal)) <= numActiveComponents\n" +
            "RETURN cs {.*, activeSignals: collect(activatedSignal {.*, timestamp: actTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignal}"

/*
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
*/
        /*
        MATCH (s:Signal {name:MqttShutdownLog})
        MATCH (comp:DataPipeline {name:})
        MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))
        FOREACH (r IN relationships(rels) | SET r.activationTime= )
        WITH cs
        MATCH ()-[signalRels:PART_OF]->(cs)
        MATCH (n)-[activeSignalRels:PART_OF]->(cs)
        WHERE EXISTS (activeSignalRels.activationTime)
        WITH n, cs, count(signalRels) as numConnectedSignals, activeSignalRels.activationTime as aTime
        RETURN cs, collect(n), collect(aTime), numConnectedSignals
        */
/* EZ JO
        MATCH (s:Signal {name:'MqttShutdownLog'})
        MATCH (comp:DataPipeline {name:'MQTT_BROKER'})
        MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))
        FOREACH (r IN relationships(rels) | SET r.activationTime=datetime('2020-02-08T18:23:19.810000000') )
        WITH cs
        MATCH ()-[signalRels:PART_OF]->(cs)
        MATCH (n)-[activeSignalRels:PART_OF]->(cs)
        WHERE EXISTS (activeSignalRels.activationTime)
        MATCH (n)-[:SIGNALLED_BY]->(cp:DataPipeline)
        WITH n, cs, count(signalRels) as numConnectedSignals, activeSignalRels.activationTime as aTime, cp.name as componentName
        RETURN cs {.*, activeSignals: collect(n {.*, timestamp: aTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignals}
        */

        /*
        MATCH (s:Signal {name:'MqttShutdownLog'})
        MATCH (comp:DataPipeline {name:'MQTT_BROKER'})
        MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal))
        FOREACH (r IN relationships(rels) | SET r.activationTime=datetime('2020-02-08T18:23:19.810000000') )
        WITH cs
        MATCH ()-[signalRels:PART_OF]->(cs)
        MATCH (n)-[activeSignalRels:PART_OF]->(cs)
        WHERE EXISTS (activeSignalRels.activationTime)
        WITH n as activatedSignal, cs, activeSignalRels.activationTime as actTime, signalRels
        MATCH (activatedSignal)-[:SIGNALLED_BY]->(cp:DataPipeline)
        WITH activatedSignal, cs, count(signalRels) as numConnectedSignals, actTime as aTime, cp.name as componentName, size(collect(activatedSignal)) as size
        RETURN cs {.*, activeSignals: collect(activatedSignal {.*, timestamp: aTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignals}, size
        */
    }
}
