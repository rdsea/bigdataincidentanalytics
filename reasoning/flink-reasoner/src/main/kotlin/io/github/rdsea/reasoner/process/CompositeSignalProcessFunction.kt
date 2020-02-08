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
        val sortedSignals = compositeSignal.activeSignals.sortedBy { it.timestamp } // TODO check sorting, most recent should be at 0
        log.info("Signals of CS sorted. ${sortedSignals.map { it.timestamp }}")
        val minRequiredActiveSignals = ceil(abs(compositeSignal.activationThreshold * compositeSignal.numOfConnectedSignals)).toInt()
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
        }
        /*
        * function input: list of CSs (the incoming Signal activated)
        * loop through CSs as cs
        *   x = determine required number of timely signals (threshold * cnt(PART_OF rels))-> round to next integer
        *   if(number of rels with a set timestamp i.e not null < x)
        *       -> break, this CS cannot be fired, process next CS
        *   else
        *       sorted = sort rels according to timestamp, item at index 0 most recent one
        *       if(Duration.between(sorted[0],sorted[x-1]).seconds < coolDownSec)
        *           Signals between [0] and [x-1] must be included in the Incident report, there will be one
        *           if (x < sorted.length)
        *               -> means there are more activated signals, but we have to check the time window for each one
        *               index = x; twViolated = false
        *               while(!twViolated && index < sorted.length)
        *                   if(Duration.between(sorted[0],sorted[index]).seconds < coolDownSeconds)
        *                       add signals at sorted[index] to the list of reported signals
        *                       index++
        *                   else
        *                       twViolated = true
        *           else
        *               -> means there are exactly as many activated signals as required, nothing more to do
        *           Generate incident report with signals, this CS; emit Incident
        *       else
        *          -> means that required signals are outside the time-window
        *
        * */
    }

    private fun areSignalsWithinTimeWindow(first: Signal, second: Signal, windowLengthSec: Int): Boolean {
        return Duration.between(first.timestamp, second.timestamp).seconds < windowLengthSec
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
