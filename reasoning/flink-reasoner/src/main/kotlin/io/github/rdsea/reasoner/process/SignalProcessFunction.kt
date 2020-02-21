package io.github.rdsea.reasoner.process

import io.github.rdsea.reasoner.dao.DAO
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.Signal
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
class SignalProcessFunction(private val dao: DAO) : ProcessFunction<Signal, CompositeSignal>() {

    private val log: Logger by lazy { LoggerFactory.getLogger(SignalProcessFunction::class.java) }
    private val sideOutputTag: OutputTag<Signal> by lazy { object : OutputTag<Signal>("side-output") {} }

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        dao.initialize()
    }

    override fun processElement(value: Signal, ctx: Context, out: Collector<CompositeSignal>) {
        val signalNode = dao.findSignalOrCreate(value)
        log.info("Signal Node: $signalNode")
        val incomingSignalTime = signalNode.timestamp
        if (signalNode.requiresMultipleOccurrences()) {
            if (signalNode.isWithinCoolDownWindow() && signalNode.isCounterInitialized()) {
                signalNode.thresholdCounter = signalNode.thresholdCounter + 1
            } else {
                signalNode.thresholdCounter = 1
            }
            signalNode.lastSignalTime = incomingSignalTime
        }
        val potentiallyActivatedCompositeSignals = dao.updateSignalAndGetActivatedCompositeSignals(signalNode)
        potentiallyActivatedCompositeSignals.forEach { out.collect(it) }
        ctx.output(sideOutputTag, signalNode)
    }

    override fun close() {
        super.close()
        dao.tearDown()
    }
}
