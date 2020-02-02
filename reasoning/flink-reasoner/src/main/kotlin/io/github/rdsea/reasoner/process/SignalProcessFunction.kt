package io.github.rdsea.reasoner.process

import io.github.rdsea.reasoner.dao.DAO
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.util.LocalDateTimeJsonSerializer.Companion.formatter
import java.lang.IllegalArgumentException
import java.time.Duration
import java.time.ZonedDateTime
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
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
class SignalProcessFunction(private val dao: DAO) : ProcessFunction<Map<String, Any>, Incident>() {

    private lateinit var log: Logger

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        dao.initialize()
        log = LoggerFactory.getLogger(SignalProcessFunction::class.java)
    }

    override fun close() {
        super.close()
        dao.tearDown()
    }

    override fun processElement(value: Map<String, Any>, ctx: Context, out: Collector<Incident>) {
        log.info("Signal Map: $value")
        val signalName = value["tag"].toString().substringAfterLast(".")
        val optional = dao.readSignalByName(signalName)
        val signalNode = if (optional.isPresent) {
            optional.get()
        } else {
            throw IllegalArgumentException("There is no signal in the knowledge base with name \"$signalName\"!")
        }
        log.info("Signal Node: $signalNode")
        val incomingSignalTime = ZonedDateTime.parse(value["event_time"].toString(), formatter).toLocalDateTime()
        var needToContinue = true
        if (signalNode.threshold != null && signalNode.coolDownSec != null) {
            signalNode.thresholdCounter = if (signalNode.lastSignalTime != null && Duration.between(
                signalNode.lastSignalTime,
                incomingSignalTime
            ).seconds >= signalNode.coolDownSec!! || signalNode.thresholdCounter == null
            ) {
                1
            } else {
                signalNode.thresholdCounter!! + 1
            }
            signalNode.lastSignalTime = incomingSignalTime
            dao.updateSignal(signalNode)
            if (signalNode.thresholdCounter!! < signalNode.threshold!!) {
                needToContinue = false
                log.info("Signal $signalName is at ${signalNode.thresholdCounter} occurrences. Will fire at ${signalNode.threshold}")
            } else {
                log.info("Signal $signalName reached the required number of occurrences.")
            }
        }
        if (needToContinue) {
            val compositeSignals = dao.readCompositeSignalsOfSignalByName(signalName)
            if (compositeSignals.isNotEmpty()) {
                compositeSignals.forEach { compSig ->
                    if (compSig.lastSignalTime != null && Duration.between(
                        compSig.lastSignalTime,
                        incomingSignalTime
                    ).seconds >= compSig.coolDownSec || compSig.activeSignals == null
                    ) {
                        compSig.activeSignals = mutableListOf(signalName)
                    } else if (compSig.activeSignals != null && !compSig.activeSignals!!.contains(signalName)) {
                        compSig.activeSignals!!.add(signalName)
                    }
                    compSig.lastSignalTime = incomingSignalTime
                    dao.updateCompositeSignal(compSig)
                    val activeSignals = compSig.activeSignals!!.toList()
                    if (activeSignals.size / compSig.numOfConnectedSignals >= compSig.activationThreshold) {
                        log.info("Reporting incident stemming from CompositeSignal \"${compSig.name}\"")
                        dao.readIncidentsAndSignalsOfCompositeSignal(compSig).forEach { out.collect(it) }
                    } else {
                        log.info("Not reporting incident because ${activeSignals.size}/${compSig.numOfConnectedSignals} < ${compSig.activationThreshold}")
                    }
                }
            } else {
                log.error("There are no composite signals associated with Signal(name:$signalName). Json:$value")
            }
        }
    }
}
