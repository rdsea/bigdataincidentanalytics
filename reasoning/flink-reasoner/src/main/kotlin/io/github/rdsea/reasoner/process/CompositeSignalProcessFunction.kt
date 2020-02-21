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
        val sortedSignals = compositeSignal.activeSignalsSorted
        val mostRecentSignal = compositeSignal.activeSignalsSorted.first()
        val minRequiredActiveSignals = ceil(abs(compositeSignal.activationThreshold * compositeSignal.numOfConnectedSignals)).toInt()
        // invariant: minRequiredSignals is guaranteed to be at least 1 or at most compositeSignal.numOfConnectedSignals
        if (areSignalsWithinTimeWindow(mostRecentSignal, sortedSignals[minRequiredActiveSignals - 1], compositeSignal.coolDownSec)) {
            // at this point there will be a generated Incident report, because there are (at least) minRequiredActiveSignals
            // within time window defined by coolDownSec
            var indexOfInclusion = minRequiredActiveSignals
            if (minRequiredActiveSignals < sortedSignals.size) {
                // means that there may be more, older, activated signals potentially within the time window, need to check
                while (indexOfInclusion < sortedSignals.size &&
                    areSignalsWithinTimeWindow(mostRecentSignal, sortedSignals[indexOfInclusion], compositeSignal.coolDownSec)
                ) {
                    indexOfInclusion++
                }
            }
            // this is a crucial step: when building incident reports, we want to include only those active signals,
            // which are indeed within the CompositeSignal's time window. Therefore we only propagate up to
            // indexOfInclusion amount of signals. SIDE NOTE: the correct OOP approach would be to create a new CompositeSignal
            // object with the reduced activeSignalsSorted list to adhere immutability. Here however, we give this up in favor
            // of performance (reduce Flink's object serialization etc.).
            compositeSignal.activeSignalsSorted = sortedSignals.take(indexOfInclusion)
            buildIncidents(compositeSignal)
                .forEach { out.collect(it) }
        } else {
            log.info("CS \"${compositeSignal.name}\" not fired. Most recent signal and signal at position ${minRequiredActiveSignals - 1} outside ${compositeSignal.coolDownSec}s time window: [${sortedSignals[0].timestamp}, ${sortedSignals[minRequiredActiveSignals - 1].timestamp}]")
            val signalsToReset = sortedSignals
                .filter { !areSignalsWithinTimeWindow(mostRecentSignal, it, compositeSignal.coolDownSec) }
                .toList()
            dao.resetSignalActivationForCompositeSignal(signalsToReset, compositeSignal)
        }
    }

    private fun areSignalsWithinTimeWindow(first: Signal, second: Signal, windowLengthSec: Int): Boolean {
        return Duration.between(first.timestamp, second.timestamp).abs().seconds < windowLengthSec
    }

    private fun buildIncidents(compositeSignal: CompositeSignal): List<Incident> {
        return dao
            .findIncidentsOfCompositeSignal(compositeSignal)
            .map {
                Incident(it.name, compositeSignal.activeSignalsSorted.first().timestamp, compositeSignal)
            }
    }

    override fun close() {
        super.close()
        dao.tearDown()
    }
}
