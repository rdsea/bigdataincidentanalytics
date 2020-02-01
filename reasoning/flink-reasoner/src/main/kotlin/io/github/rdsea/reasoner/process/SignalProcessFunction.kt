package io.github.rdsea.reasoner.process

import io.github.rdsea.reasoner.Main.Companion.driver
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.IncidentReport
import io.github.rdsea.reasoner.domain.SignalNode
import java.lang.IllegalArgumentException
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
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
class SignalProcessFunction : ProcessFunction<Map<String, Any>, IncidentReport>() {

    private lateinit var log: Logger
    private lateinit var formatter: DateTimeFormatter

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        log = LoggerFactory.getLogger(SignalProcessFunction::class.java)
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.systemDefault())
    }

    /**
     * Process one element from the input stream.
     *
     *
     * This function can output zero or more elements using the [Collector] parameter
     * and also update internal state or set timers using the [Context] parameter.
     *
     * @param value The input value.
     * @param ctx A [Context] that allows querying the timestamp of the element and getting
     * a [TimerService] for registering timers and querying the time. The
     * context is only valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     * to fail and may trigger recovery.
     */
    override fun processElement(value: Map<String, Any>, ctx: Context, out: Collector<IncidentReport>) {
        log.info("Signal Map: $value")
        val signalId = value["tag"].toString().substringAfterLast(".")
        val signalNode = getSignalNode(signalId)
        log.info("Signal Node: $signalNode")
        val incomingSignalTime = ZonedDateTime.parse(value["event_time"].toString(), formatter).toLocalDateTime()
        //  if signal has a number of required occurrences ["threshold"]
        //      if lastSignalTime != null && time_diff(lastSignalTime, now) >= cooldownSec
        //          set threshold_counter to 1
        //      else
        //          increment threshold_counter
        //      set lastSignalTime to currentTime (or event_time)
        //      if threshold_counter >= threshold
        //          we need to continue with CSs
        //      else
        //          break; means signal must occur again in order to fire
        var needToContinue = true
        if (signalNode.threshold != null && signalNode.coolDownSec != null) {
            val newThresholdCounterValue: Int = if (signalNode.lastSignalTime != null && Duration.between(signalNode.lastSignalTime, incomingSignalTime).seconds >= signalNode.coolDownSec!! || signalNode.thresholdCounter == null) {
                // means that the last signal has lost its significance according to the cool-down time
                // and the counter needs to be reset. We add +1 because of this signal
                // OR that the threshold counter has not yet been initialized
                1
            } else {
                // we need to have default 0 because maybe the counter has not yet been initialized
                signalNode.thresholdCounter!! + 1
            }
            updateNodeThresholdCounterAndSignalTime(signalId, newThresholdCounterValue, incomingSignalTime)
            if (newThresholdCounterValue < signalNode.threshold!!) {
                // we don't need to process CompositeSignals, because this signal requires more occurrences in order to fire
                needToContinue = false
                log.info("Signal $signalId is at $newThresholdCounterValue occurrences. Will fire at ${signalNode.threshold}")
            } else {
                log.info("Signal $signalId reached the required number of occurrences.")
            }
        }
        if (needToContinue) {
            // get all corresponding CSs
            // for each CS
            //  if lastSignalTime != null && time_diff(lastSignalTime,now) >= cooldownSec
            //      set activeSignals to an empty list and add this signal's name
            //  set lastSignalTime to event_time
            //  if activeSignals already contains this signal's name
            //      do nothing
            //  else
            //      add this signal's name to list of activeSignals
            //      if activeSignals.size() >= activation_threshold
            //          get all Incidents this CS lead_to
            //          for each incident
            //              create incident report, include CS, activated signals
            //              (optionally) query for other incidents this incident may lead to and include in report
            //              emit incident report via Collector
            //      else
            //          do nothing, incidents for this CS not activated yet
            val compositeSignals = getCorrespondingCompositeSignals(signalId)
            if (compositeSignals.isNotEmpty()) {
                compositeSignals.forEach { compSig ->
                    if (compSig.lastSignalTime != null && Duration.between(compSig.lastSignalTime, incomingSignalTime).seconds >= compSig.coolDownSec || compSig.activeSignals == null) {
                        compSig.activeSignals = listOf(signalId).toMutableList()
                    } else if (compSig.activeSignals != null && !compSig.activeSignals!!.contains(signalId)) {
                        compSig.activeSignals!!.add(signalId)
                    }
                    compSig.lastSignalTime = incomingSignalTime
                    updateCompositeSignal(compSig)
                    val activeSignals = compSig.activeSignals!!.toList()
                    if (activeSignals.size / compSig.numOfConnectedSignals >= compSig.activationThreshold) {
                        log.info("Reporting incident stemming from CompositeSignal \"${compSig.name}\"")
                        out.collect(IncidentReport("This is a test incident report"))
                    } else {
                        log.info("Not reporting incident because ${activeSignals.size}/${compSig.numOfConnectedSignals} < ${compSig.activationThreshold}")
                    }
                }
            } else {
                log.error("There are no composite signals associated with Signal(name:$signalId). Json:$value")
            }
        }
    }

    private fun getSignalNode(signalName: String): SignalNode {
        driver.session().use { session ->
            val result: Result = session.run(
                "MATCH (s:Signal {name:\$param}) RETURN s",
                parameters("param", signalName)
            )
            // Each Cypher execution returns a stream of records.
            if (result.hasNext()) {
                val node: Node = result.next().get("s").asNode()
                return SignalNode.fromNeoNode(node)
            }
            throw IllegalArgumentException("There is no Signal node with name $signalName!!!")
        }
    }

    private fun updateNodeThresholdCounterAndSignalTime(signalName: String, thresholdCounterValue: Int, lastSignalTime: LocalDateTime) {
        driver.session().use { session ->
            session?.writeTransaction { tx ->
                tx.run("MATCH (s:Signal {name:\$x}) SET s.thresholdCounter = \$y SET s.lastSignalTime = \$z",
                    parameters("x", signalName, "y", thresholdCounterValue, "z", lastSignalTime))
            }
        }
    }

    private fun getCorrespondingCompositeSignals(signalName: String): List<CompositeSignal> {
        driver.session().use { session ->
            val result: Result = session!!.run(
                "MATCH (s:Signal {name:\$x}) " +
                        "MATCH (s)-[:PART_OF]->(n:CompositeSignal) " +
                        "MATCH ()-[r:PART_OF]->(n:CompositeSignal) " +
                        "WITH n,count(r) as cnt " +
                        "RETURN n,cnt",
                parameters("x", signalName)
            )
            // Each Cypher execution returns a stream of records.
            val signals = mutableListOf<CompositeSignal>()
            while (result.hasNext()) {
                val record: Record = result.next()
                /*val node = record.get("n").asNode()
                val signal = CompositeSignal(node.get("name").asString(), node.get("coolDownSec").asInt(3600),
                    node.get("activationThreshold").asDouble(1.0),
                    record["cnt"].asInt(),
                    node.get("lastSignalTime").asLocalDateTime(null),
                    node.get("activeSignals").asList({ t: Value? -> t?.asString() }, null))*/
                val signal = CompositeSignal.fromNeoRecord(record)
                log.info("CompositeSignal: $signal")
                signals.add(signal)
            }
            return signals
        }
    }

    private fun updateCompositeSignal(compositeSignal: CompositeSignal) {
        driver.session().use { session ->
            session.writeTransaction { tx ->
                tx.run("MATCH (s:CompositeSignal {name:\$x}) " +
                        "SET s.lastSignalTime = \$y " +
                        "SET s.activeSignals = \$z",
                    parameters("x", compositeSignal.name, "y", compositeSignal.lastSignalTime, "z", compositeSignal.activeSignals))
            }
        }
    }
}
