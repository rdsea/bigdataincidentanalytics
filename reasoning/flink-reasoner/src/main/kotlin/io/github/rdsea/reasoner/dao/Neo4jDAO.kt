package io.github.rdsea.reasoner.dao

import com.google.gson.Gson
import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.domain.IncidentEntity
import io.github.rdsea.reasoner.domain.Signal
import java.io.Serializable
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

    override fun findSignalOrCreate(signal: Signal): Signal {
        driver.session().use { session ->
            val result: Result = session.run(
                FIND_OR_CREATE_SIGNAL_QUERY,
                parameters("name", signal.name, "component", signal.pipelineComponent)
            )
            if (result.hasNext()) {
                val node: Node = result.next().get("s").asNode()
                val persistedSignal = gson.fromJson(gson.toJson(node.asMap()), Signal::class.java)
                return mergeSignals(signal, persistedSignal)
            }
            throw IllegalStateException("Unable to find or create signal $signal")
        }
    }

    override fun updateSignalAndGetActivatedCompositeSignals(signal: Signal): List<CompositeSignal> {
        driver.session().use { session ->
            return session.writeTransaction { tx ->
                val res = tx.run(
                    if (signal.isActivated()) UPDATE_SIGNAL_WITH_TRIGGER_QUERY else UPDATE_SIGNAL_WITHOUT_TRIGGER_QUERY,
                    parameters(
                        "name", signal.name, "component", signal.pipelineComponent,
                        "counter", if (signal.thresholdCounter == -1) null else { signal.thresholdCounter },
                        "time", signal.timestamp,
                        "summary", signal.summary, "details", gson.toJson(signal.details),
                        "defaultActivationThreshold", CompositeSignal.DEFAULT_ACTIVATION_THRESHOLD
                    )
                )
                if (!signal.isActivated()) {
                    emptyList()
                } else {
                    val compositeSignals = mutableListOf<CompositeSignal>()
                    while (res.hasNext()) {
                        val record: Record = res.next()
                        val cs = gson.fromJson(gson.toJsonTree(record["cs"].asMap()), CompositeSignal::class.java)
                        cs.activeSignalsSorted = cs.activeSignalsSorted.sortedByDescending { it.timestamp }
                        log.info("parsed composite signal: $cs")
                        compositeSignals.add(cs)
                    }
                    compositeSignals
                }
            }
        }
    }

    override fun resetSignalActivationForCompositeSignal(signals: List<Signal>, compositeSignal: CompositeSignal) {
        val signalsNameList = mutableListOf<String>()
        val signalComponentsList = mutableListOf<String>()
        signals.forEach {
            signalsNameList.add(it.name)
            signalComponentsList.add(it.pipelineComponent)
        }
        driver.session().use { session ->
            return session.writeTransaction { tx ->
                tx.run(
                    RESET_SIGNALS_ACTIVATION_TIME_QUERY,
                    parameters(
                        "signalNamesList", signalsNameList, "signalComponentsList", signalComponentsList,
                        "compSigName", compositeSignal.name
                    )
                )
            }
        }
    }

    override fun findIncidentsOfCompositeSignal(compositeSignal: CompositeSignal): List<IncidentEntity> {
        driver.session().use { session ->
            val result: Result = session.run(
                INCIDENTS_OF_COMPOSITE_SIGNALS_QUERY,
                parameters(
                    "name", compositeSignal.name,
                    "compSignalTime", compositeSignal.activeSignalsSorted.first().timestamp
                )
            )
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
        // Tries to find a signal with the given name and connected pipeline component and creates it if it doesn't exist.
        private const val MATCH_OR_CREATE_SIGNAL_BY_COMP = "MATCH (dp:DataPipeline{name:\$component}) " +
            "MATCH (e:Element{name:'Signal'}) " +
            "MERGE (s:Signal {name:\$name})-[:SIGNALLED_BY]->(dp) " +
            "MERGE (e)-[:IS]->(s) "

        // Returns the Signal node(s) found by the above query
        private const val FIND_OR_CREATE_SIGNAL_QUERY = MATCH_OR_CREATE_SIGNAL_BY_COMP + "RETURN s"

        // Simply updates a Signal's properties that is identified by its name and corresponding pipeline component
        private const val UPDATE_SIGNAL_WITHOUT_TRIGGER_QUERY = MATCH_OR_CREATE_SIGNAL_BY_COMP +
            "SET s.thresholdCounter = \$counter " +
            "SET s.lastSignalTime = \$time " +
            "SET s.summary = \$summary " +
            "SET s.details = \$details \n"

        // Returns a list of incident nodes the CompositeSignal with the given name indicates,
        // BUT only those that haven't already been reported in the last RE_NOTIFICATION_WAIT_SEC seconds
        private const val INCIDENTS_OF_COMPOSITE_SIGNALS_QUERY = "MATCH (cs:CompositeSignal {name:\$name}) " +
            "MATCH (cs)-[r:INDICATES]->(incident:Incident) " +
            "WHERE duration.inSeconds(COALESCE(r.lastActivation,localdatetime('1990-01-01T00:00:00')),\$compSignalTime).seconds > ${Incident.RE_NOTIFICATION_WAIT_SEC} " +
            "SET r.lastActivation=\$compSignalTime " +
            "RETURN collect(incident)"

        // For a given CompositeSignal and a list of Signals (expressed through a list of Signal names and a list of
        // pipeline component names) this query removes the 'activationTime' property from the relationship between the
        // CompositeSignal and each given Signal.
        private const val RESET_SIGNALS_ACTIVATION_TIME_QUERY = "WITH \$signalNamesList as signalNames, " +
            "\$signalComponentsList as signalComponents " +
            "UNWIND range(0,size(signalNames)-1) as i " +
            "MATCH (Element {name:'CompositeSignal'})-[:IS]->(cs:CompositeSignal {name:\$compSigName}) " +
            "MATCH (s:Signal)-[:SIGNALLED_BY]->(pc:DataPipeline) " +
            "WHERE s.name=signalNames[i] AND pc.name=signalComponents[i] " +
            "MATCH (s)-[r:PART_OF]->(cs) " +
            "SET r.activationTime=null"

        // Updates a Signal's properties and further collects all CompositeSignals this signal may have potentially
        // triggered. For all CompositeSignal this Signal is connected to, the query
        // (i) updates the activationTime of the PART_OF relationship
        // (ii) filters out the CompositeSignals that don't have "enough" activated signals - which is determined by
        //      the number of connected nodes, the number of set (i.e. != null) activatedTime properties and
        //      the activationThreshold of the CompositeSignal
        // (iii) The result is collected into a list of CompositeSignal and for each Signal the pipeline component and
        // the timestamp properties are also derived from their relationships
        private const val UPDATE_SIGNAL_WITH_TRIGGER_QUERY = UPDATE_SIGNAL_WITHOUT_TRIGGER_QUERY +
            "WITH s " +
            "MATCH rels = ((s)-[:PART_OF]->(cs:CompositeSignal)) " +
            "FOREACH (r IN relationships(rels) | SET r.activationTime=\$time ) " +
            "WITH cs " +
            "MATCH ()-[signalRels:PART_OF]->(cs) " +
            "WITH cs, count(signalRels) as numConnectedSignal " +
            "MATCH (n)-[activeSignalRels:PART_OF]->(cs), (n)-[:SIGNALLED_BY]->(comp:DataPipeline) " +
            "WHERE EXISTS (activeSignalRels.activationTime) " +
            "WITH cs, n as activatedSignal, numConnectedSignal, activeSignalRels.activationTime as actTime, comp.name as componentName " +
            "MATCH ()-[r:PART_OF]->(cs) " +
            "WHERE EXISTS (r.activationTime) " +
            "WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, count(r) as numActiveComponents " +
            "MATCH (cs) " +
            "WITH cs, activatedSignal, numConnectedSignal, actTime, componentName, numActiveComponents " +
            "WHERE toInteger(ceil(COALESCE(cs.activationThreshold,\$defaultActivationThreshold) * numConnectedSignal)) <= numActiveComponents " +
            "RETURN cs {.*, activeSignals: collect(activatedSignal {.*, timestamp: actTime, pipelineComponent: componentName}), numOfConnectedSignals: numConnectedSignal}"
    }
}
