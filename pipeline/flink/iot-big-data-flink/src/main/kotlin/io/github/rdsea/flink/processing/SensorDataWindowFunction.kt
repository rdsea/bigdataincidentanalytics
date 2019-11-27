package io.github.rdsea.flink.processing

import io.github.rdsea.flink.FLUENCY
import io.github.rdsea.flink.FLUENTD_PREFIX
import io.github.rdsea.flink.domain.WindowedSensorReport
import io.github.rdsea.flink.domain.SensorRecord
import io.github.rdsea.flink.util.CloudEventDateTimeFormatter
import io.github.rdsea.flink.util.LocalDateTimeJsonSerializer
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.komamitsu.fluency.EventTime
import java.time.Instant
import java.util.UUID

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class SensorDataWindowFunction : WindowFunction<SensorRecord, WindowedSensorReport, String, GlobalWindow> {

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
        out: Collector<WindowedSensorReport>
    ) {
        val records = input.iterator().asSequence().toList()
        val reportId = UUID.randomUUID().toString()
        val result = WindowedSensorReport(reportId, key, records.size, records.map { it.humidity }.average(), records.map { it.temperature }.average(),
            records.map { it.time.format(LocalDateTimeJsonSerializer.formatter) })

        val instant = Instant.now()
        FLUENCY.emit("$FLUENTD_PREFIX.aggregation.app.dataAsset",
            EventTime.fromEpoch(instant.epochSecond, instant.nano.toLong()),
            mapOf(
                Pair("specversion", "0.3"),
                Pair("id", UUID.randomUUID().toString()),
                Pair("type", "$FLUENTD_PREFIX.mqtt.app.dataAsset"),
                Pair("source", "flink:WindowFunction/${javaClass.simpleName}"),
                Pair("time", CloudEventDateTimeFormatter.format(instant)),
                Pair("subject", result.id),
                Pair("data", result),
                Pair("message", "Periodic aggregation of humidity and temperature of sensor deviceId: $key")
            )
        )
        out.collect(result)
    }
}