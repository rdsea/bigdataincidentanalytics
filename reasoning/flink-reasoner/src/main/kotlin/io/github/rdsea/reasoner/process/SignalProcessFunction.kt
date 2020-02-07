package io.github.rdsea.reasoner.process

import io.github.rdsea.reasoner.dao.DAO
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.domain.Signal
import java.lang.IllegalArgumentException
import java.time.Duration
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
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
class SignalProcessFunction(private val dao: DAO) : ProcessFunction<Signal, Incident>() {

    private lateinit var log: Logger
    private lateinit var sideOutputTag: OutputTag<Signal>

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        dao.initialize()
        log = LoggerFactory.getLogger(SignalProcessFunction::class.java)
        sideOutputTag = object : OutputTag<Signal>("side-output") {}
    }

    override fun close() {
        super.close()
        dao.tearDown()
    }

    override fun processElement(value: Signal, ctx: Context, out: Collector<Incident>) {
        val optional = dao.findSignal(value)
        val signalNode = if (optional.isPresent) {
            optional.get()
        } else {
            log.info("Signal: $value")
            throw IllegalArgumentException("There is no signal in the knowledge base with name \"${value.name}\" and component ${value.pipelineComponent}!")
        }
        log.info("Signal Node: $signalNode")
        val incomingSignalTime = signalNode.timestamp
        var needToContinue = true
        if (signalNode.threshold > 0 && signalNode.coolDownSec > 0) {
            if (signalNode.lastSignalTime != null && Duration.between(
                signalNode.lastSignalTime,
                incomingSignalTime
            ).seconds >= signalNode.coolDownSec || signalNode.thresholdCounter < 0
            ) {
                signalNode.thresholdCounter = 1
            } else {
                signalNode.thresholdCounter = signalNode.thresholdCounter + 1
            }
            signalNode.lastSignalTime = incomingSignalTime
            if (signalNode.thresholdCounter < signalNode.threshold) {
                needToContinue = false
                log.info("Signal ${signalNode.name} is at ${signalNode.thresholdCounter} occurrences. Will fire at ${signalNode.threshold}")
            } else {
                log.info("Signal ${signalNode.name} reached the required number of occurrences.")
            }
        }
        dao.updateSignal(signalNode)
        if (needToContinue) {
            val compositeSignals = dao.findCompositeSignalsOfSignal(signalNode)
            if (compositeSignals.isNotEmpty()) {
                val signalKey = "${signalNode.name}_${signalNode.pipelineComponent}"
                compositeSignals.forEach { compSig ->
                    if (compSig.lastSignalTime != null && Duration.between(
                        compSig.lastSignalTime,
                        incomingSignalTime
                    ).seconds >= compSig.coolDownSec || compSig.activeSignals == null
                    ) {
                        compSig.activeSignals = mutableListOf(signalKey)
                    } else if (compSig.activeSignals != null && !compSig.activeSignals!!.contains(signalKey)) {
                        compSig.activeSignals!!.add(signalKey)
                    }
                    compSig.lastSignalTime = incomingSignalTime
                    dao.updateCompositeSignal(compSig)
                    val activeSignals = compSig.activeSignals!!.toList()
                    if (activeSignals.size / compSig.numOfConnectedSignals >= compSig.activationThreshold) {
                        log.info("Reporting incident stemming from CompositeSignal \"${compSig.name}\"")
                        dao.findIncidentsOfCompositeSignal(compSig).forEach { out.collect(it) }
                    } else {
                        log.info("Not reporting incident because ${activeSignals.size}/${compSig.numOfConnectedSignals} < ${compSig.activationThreshold}")
                    }
                }
            } else {
                log.error("There are no composite signals associated with signal ${signalNode.name}.")
            }
        }
        ctx.output(sideOutputTag, signalNode)
    }
}
