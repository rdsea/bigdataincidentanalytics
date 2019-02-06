package io.github.rdsea.flink.processing

import io.github.rdsea.flink.domain.SensorAlarmReport
import io.github.rdsea.flink.domain.SensorRecord
import mu.KLogging
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class SensorDataWindowFunction : WindowFunction<SensorRecord, SensorAlarmReport, String, GlobalWindow> {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key The key for which this window is evaluated.
     * @param window The window that is being evaluated.
     * @param input The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     *
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    override fun apply(
        key: String,
        window: GlobalWindow,
        input: MutableIterable<SensorRecord>,
        out: Collector<SensorAlarmReport>
    ) {
        logger.info { "WindowFunction for station $key entered" }
        val records = input.iterator().asSequence().toList()
        val result = SensorAlarmReport(key, records.size, records.map { it.sensorValue }.average())
        out.collect(result)
    }
    companion object : KLogging()
}