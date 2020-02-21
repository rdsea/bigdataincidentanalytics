package io.github.rdsea.reasoner.source

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.reflect.TypeToken
import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.Signal
import io.github.rdsea.reasoner.domain.SignalType
import io.github.rdsea.reasoner.util.LocalDateTimeJsonSerializer
import java.lang.IllegalArgumentException
import java.lang.reflect.Type
import java.time.LocalDateTime
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class KafkaRecordMapFunction : RichMapFunction<String, Signal>() {

    private val genericMapType: Type by lazy { object : TypeToken<Map<String, Any>>() {}.type }
    private lateinit var gson: Gson

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        gson = Main.gson
    }

    override fun map(value: String): Signal {
        val json: JsonObject = gson.fromJson(value, JsonElement::class.java).asJsonObject
        checkPreConditions(json)
        return when (val signalType = json["signal_type"].asString) {
            SignalType.LOG.name -> parseLogSignal(json)
            SignalType.PROMETHEUS_ALERT.name -> parsePrometheusSignal(json)
            else -> throw IllegalArgumentException("Unsupported signal type \"$signalType\" in Kafka record")
        }
    }

    private fun checkPreConditions(record: JsonObject) {
        if (!record.has("signal_type")) {
            throw IllegalArgumentException("\"Kafka record doesn't contain required field \\\"signal_type\\\"!\"")
        }
    }

    private fun parseLogSignal(jsonObject: JsonObject): Signal {
        val result = Signal(
            type = SignalType.LOG,
            name = jsonObject["tag"].asString.substringAfterLast("."),
            timestamp = LocalDateTime.parse(jsonObject["event_time"].asString, LocalDateTimeJsonSerializer.formatter),
            pipelineComponent = jsonObject["pipeline_component"].asString,
            summary = jsonObject["message"].asString
        )
        val details = gson.fromJson<Map<String, Any>>(jsonObject, genericMapType).toMutableMap()
        details.remove("signal_type")
        details.remove("tag")
        details.remove("event_time")
        details.remove("pipeline_component")
        details.remove("message")
        result.details = details
        return result
    }

    private fun parsePrometheusSignal(jsonObject: JsonObject): Signal {
        val labels = jsonObject["labels"].asJsonObject
        val annotations = jsonObject["annotations"].asJsonObject
        val result = Signal(
            type = SignalType.PROMETHEUS_ALERT,
            name = labels["alertname"].asString,
            timestamp = LocalDateTime.parse(jsonObject["startsAt"].asString.dropLast(1)),
            pipelineComponent = labels["pipeline_component"].asString,
            summary = annotations["summary"].asString
        )
        labels.remove("alertname")
        labels.remove("pipeline_component")
        annotations.remove("summary")
        result.details = mapOf(
            "labels" to gson.fromJson<Map<String, Any>>(labels, genericMapType),
            "annotations" to gson.fromJson<Map<String, Any>>(annotations, genericMapType)
        )
        return result
    }
}
