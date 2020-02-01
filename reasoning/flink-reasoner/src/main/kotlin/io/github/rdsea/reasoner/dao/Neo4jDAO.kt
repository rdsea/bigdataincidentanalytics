package io.github.rdsea.reasoner.dao

import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.SignalNode
import java.io.Serializable
import java.util.Optional
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Value
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

    override fun initialize() {
        driver = Main.getNeo4jDriver()
        log = LoggerFactory.getLogger(Neo4jDAO::class.java)
    }

    override fun readSignalByName(signalName: String): Optional<SignalNode> {
        driver.session().use { session ->
            val result: Result = session.run(READ_SIGNAL_QUERY, parameters("param", signalName))
            if (result.hasNext()) {
                val node: Node = result.next().get("s").asNode()
                return Optional.of(parseNeoNode(node))
            }
            return Optional.empty()
        }
    }

    override fun updateSignal(signal: SignalNode) {
        driver.session().use { session ->
            session.writeTransaction { tx ->
                tx.run(
                    UPDATE_SIGNAL_QUERY,
                    parameters("x", signal.name, "y", signal.thresholdCounter, "z", signal.lastSignalTime)
                )
            }
        }
    }

    override fun readCompositeSignalsOfSignalByName(signalName: String): List<CompositeSignal> {
        driver.session().use { session ->
            val result: Result = session.run(READ_CS_OF_SIGNAL_QUERY, parameters("x", signalName))
            val signals = mutableListOf<CompositeSignal>()
            while (result.hasNext()) {
                val record: Record = result.next()
                val signal = parseNeoRecord(record)
                signals.add(signal)
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

    override fun tearDown() {
        driver.close()
    }

    private fun parseNeoRecord(record: Record): CompositeSignal {
        val numOfConnectedSignals = record["cnt"].asInt()
        val node = record.get("n").asNode()
        val signals =
            node.get("activeSignals").asList({ t: Value? -> t!!.asString() }, null)?.toMutableList()
        return CompositeSignal(
            node.get("name").asString(), node.get("coolDownSec").asInt(3600),
            node.get("activationThreshold").asDouble(1.0), numOfConnectedSignals,
            node.get("lastSignalTime").asLocalDateTime(null), signals
        )
    }

    private fun parseNeoNode(node: Node): SignalNode {
        val threshold: Int? = getIntValueOrNull(node, "threshold")
        val thresholdCounter: Int? = getIntValueOrNull(node, "thresholdCounter")
        val coolDownSec: Int? = getIntValueOrNull(node, "coolDownSec")
        return SignalNode(
            node["name"].asString(), threshold, thresholdCounter,
            node["lastSignalTime"].asLocalDateTime(null), coolDownSec
        )
    }

    private fun getIntValueOrNull(node: Node, key: String): Int? {
        return if (node[key].isNull) {
            null
        } else {
            node[key].asInt()
        }
    }

    companion object {
        private const val serialVersionUID = 20180617104400L
        private const val READ_SIGNAL_QUERY = "MATCH (s:Signal {name:\$param}) RETURN s"
        private const val UPDATE_SIGNAL_QUERY = "MATCH (s:Signal {name:\$x}) SET s.thresholdCounter = \$y " +
            "SET s.lastSignalTime = \$z"
        private const val READ_CS_OF_SIGNAL_QUERY = "MATCH (s:Signal {name:\$x}) " +
            "MATCH (s)-[:PART_OF]->(n:CompositeSignal) " +
            "MATCH ()-[r:PART_OF]->(n:CompositeSignal) " +
            "WITH n,count(r) as cnt " +
            "RETURN n,cnt"
        private const val UPDATE_COMP_SIGNAL_QUERY = "MATCH (s:CompositeSignal {name:\$x}) " +
            "SET s.lastSignalTime = \$y " +
            "SET s.activeSignals = \$z"
    }
}
