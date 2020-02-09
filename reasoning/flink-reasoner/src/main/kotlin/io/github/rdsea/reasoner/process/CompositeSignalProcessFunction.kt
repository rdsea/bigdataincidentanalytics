package io.github.rdsea.reasoner.process

import io.github.rdsea.reasoner.dao.DAO
import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.domain.Signal
import java.time.Duration
import kotlin.math.abs
import kotlin.math.ceil
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
class CompositeSignalProcessFunction(private val dao: DAO) : ProcessFunction<CompositeSignal, Incident>() {

    private lateinit var log: Logger

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        dao.initialize()
        log = LoggerFactory.getLogger(SignalProcessFunction::class.java)
    }

    override fun processElement(compositeSignal: CompositeSignal, ctx: Context, out: Collector<Incident>) {
        val sortedSignals = compositeSignal.activeSignals.sortedByDescending { it.timestamp } // TODO check sorting, most recent should be at 0
        val minRequiredActiveSignals = ceil(abs(compositeSignal.activationThreshold * compositeSignal.numOfConnectedSignals)).toInt()
        log.info(
            "$minRequiredActiveSignals Signals within ${compositeSignal.coolDownSec}s required.\n" +
                "Signals sorted: ${sortedSignals.map { it.timestamp }}"
        )
        // invariant: minRequiredSignals is guaranteed to be at least 1 or at most compositeSignal.numOfConnectedSignals
        if (areSignalsWithinTimeWindow(sortedSignals[0], sortedSignals[minRequiredActiveSignals - 1], compositeSignal.coolDownSec)) {
            // at this point there will be a generated Incident report, because there are (at least) minRequiredActiveSignals
            // within time window defined by coolDownSec
            var indexOfInclusion = minRequiredActiveSignals
            if (minRequiredActiveSignals < sortedSignals.size) {
                // means that there may be more, older, activated signals potentially within the time window, need to check
                while (indexOfInclusion < sortedSignals.size &&
                    areSignalsWithinTimeWindow(sortedSignals[0], sortedSignals[indexOfInclusion], compositeSignal.coolDownSec)
                ) {
                    indexOfInclusion++
                }
            }
            compositeSignal.activeSignals = sortedSignals.take(indexOfInclusion)
            buildIncidents(compositeSignal)
                .forEach { out.collect(it) }
        } else {
            log.info("Signal 0 and signal ${minRequiredActiveSignals - 1} outside time window: [${sortedSignals[0].timestamp}, ${sortedSignals[minRequiredActiveSignals - 1].timestamp}]")
        }
    }

    private fun areSignalsWithinTimeWindow(first: Signal, second: Signal, windowLengthSec: Int): Boolean {
        return Duration.between(first.timestamp, second.timestamp).abs().seconds < windowLengthSec
    }

    private fun buildIncidents(compositeSignal: CompositeSignal): List<Incident> {
        return dao
            .findIncidentsOfCompositeSignal(compositeSignal)
            .map {
                Incident(it.name, compositeSignal.activeSignals[0].timestamp, compositeSignal)
            }
    }

    override fun close() {
        super.close()
        dao.tearDown()
    }
}
