package io.github.rdsea.reasoner.dao

import com.google.gson.Gson
import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.Incident
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

    override fun updateSignal(signal: Signal) {
        driver.session().use { session ->
            session.writeTransaction { tx ->
                tx.run(
                    UPDATE_SIGNAL_QUERY,
                    parameters(
                        "name", signal.name, "component", signal.pipelineComponent,
                        "counter", if (signal.thresholdCounter == -1) null else { signal.thresholdCounter },
                        "time", signal.lastSignalTime,
                        "summary", signal.summary, "details", gson.toJson(signal.details)
                    )
                )
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
        driver.session().use { session ->
            session.writeTransaction { tx ->
                tx.run(
                    UPDATE_COMP_SIGNAL_QUERY,
                    parameters(
                        "x", compositeSignal.name, "y", compositeSignal.lastSignalTime, "z",
                        compositeSignal.activeSignals
                    )
                )
            }
        }
    }

    override fun findIncidentsOfCompositeSignal(compositeSignal: CompositeSignal): List<Incident> {
        driver.session().use { session ->
            val result: Result = session.run(INCIDENTS_AND_SIGNALS_QUERY, parameters("name", compositeSignal.name))
            val incidents = mutableListOf<Incident>()
            while (result.hasNext()) {
                val record: Record = result.next()
                val incidentNode = record["incident"].asNode()
                val signalsNodeList = record["collect(sig)"].asList { it.asNode() }
                val signalsList = signalsNodeList.map { parseNeoSignalNode(it) }
                incidents.add(Incident(incidentNode.get("name").asString(), compositeSignal.lastSignalTime!!, compositeSignal, signalsList))
            }
            return incidents
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
        private const val FIND_SIGNAL_QUERY = "MATCH (s:Signal {name:\$name}) " +
            "MATCH (s)-[:SIGNALLED_BY]->(:DataPipeline {name:\$component}) " +
            "RETURN s"
        private const val UPDATE_SIGNAL_QUERY = "MATCH (s:Signal {name:\$name}) " +
            "MATCH (s)-[:SIGNALLED_BY]->(:DataPipeline {name:\$component}) " +
            "SET s.thresholdCounter = \$counter " +
            "SET s.lastSignalTime = \$time " +
            "SET s.summary = \$summary " +
            "SET s.details = \$details"
        private const val FIND_CS_OF_SIGNAL_QUERY = "MATCH (s:Signal {name:\$name}) " +
            "MATCH (s)-[:SIGNALLED_BY]->(:DataPipeline {name:\$component}) " +
            "MATCH (s)-[:PART_OF]->(n:CompositeSignal) " +
            "MATCH ()-[r:PART_OF]->(n:CompositeSignal) " +
            "WITH n,count(r) as cnt " +
            "RETURN n,cnt"
        private const val UPDATE_COMP_SIGNAL_QUERY = "MATCH (s:CompositeSignal {name:\$x}) " +
            "SET s.lastSignalTime = \$y " +
            "SET s.activeSignals = \$z"
        private const val INCIDENTS_AND_SIGNALS_QUERY = "MATCH (cs:CompositeSignal {name:\$name})\n" +
            "MATCH (cs)-[:INDICATES]->(incident:Incident)\n" +
            "UNWIND cs.activeSignals as s\n" +
            "MATCH (sig:Signal {name:s})\n" +
            "RETURN incident,collect(sig)"
    }
}
