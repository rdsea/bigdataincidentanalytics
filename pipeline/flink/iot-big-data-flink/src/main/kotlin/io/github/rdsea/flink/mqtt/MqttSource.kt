package io.github.rdsea.flink.mqtt

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.github.rdsea.flink.FLUENCY
import io.github.rdsea.flink.FLUENTD_PREFIX
import io.github.rdsea.flink.util.CloudEventDateTimeFormatter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.fusesource.mqtt.client.MQTT
import org.fusesource.mqtt.client.QoS
import org.fusesource.mqtt.client.Topic
import org.komamitsu.fluency.EventTime
import java.net.URI
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class MqttSource(
    private val uri: URI,
    private val topic: String
) : RichSourceFunction<MqttMessage>() {

    private val interrupted = AtomicBoolean()
    private lateinit var mqtt: MQTT

    override fun open(parameters: Configuration?) {
        mqtt = MQTT()
    }

    override fun run(ctx: SourceFunction.SourceContext<MqttMessage>) {
        interrupted.set(false)
        mqtt.host = uri
        val blockingConnection = mqtt.blockingConnection()
        blockingConnection.connect()
        blockingConnection.subscribe(arrayOf(Topic(topic, QoS.AT_LEAST_ONCE)))

        /** Create CloudEvent, Emit to Fluentd */
        val instant = Instant.now()
        FLUENCY.emit(
            "$FLUENTD_PREFIX.mqtt.app.event",
            EventTime.fromEpoch(instant.epochSecond, instant.nano.toLong()),
            mapOf(
                Pair("specversion", "0.3"),
                Pair("id", UUID.randomUUID().toString()),
                Pair("type", "$FLUENTD_PREFIX.mqtt.app.event"),
                Pair("source", "flink:${runtimeContext.taskNameWithSubtasks}/UDF/${javaClass.simpleName}"),
                Pair("time", CloudEventDateTimeFormatter.format(instant)),
                Pair("subject", "signpost"),
                Pair("message", "Successfully connected to MQTT broker($uri, $topic)")
            )
        )

        while (blockingConnection.isConnected && !interrupted.get()) {
            val message = blockingConnection.receive()
            val mqttMessage = MqttMessage(message.topic, String(message.payload))
            val json: Map<String, Any> = gson.fromJson(mqttMessage.payload, typeToken)

            val time = Instant.now()
            FLUENCY.emit(
                "$FLUENTD_PREFIX.mqtt.app.dataAsset",
                EventTime.fromEpoch(time.epochSecond, time.nano.toLong()), mapOf(
                    Pair("specversion", "0.3"),
                    Pair("id", UUID.randomUUID().toString()),
                    Pair("type", "$FLUENTD_PREFIX.mqtt.app.dataAsset"),
                    Pair("source", "flink:${runtimeContext.taskNameWithSubtasks}/UDF/${javaClass.simpleName}"),
                    Pair("time", CloudEventDateTimeFormatter.format(time)),
                    Pair("subject", "${json["device_id"]}-${json["time"]}"),
                    Pair("message", "MQTT message received"),
                    Pair("data", json)
                )
            )
            message.ack()
            ctx.collect(mqttMessage)
        }

        blockingConnection.disconnect()
        val eventTime = Instant.now()
        FLUENCY.emit(
            "$FLUENTD_PREFIX.mqtt.app.event",
            EventTime.fromEpoch(eventTime.epochSecond, eventTime.nano.toLong()),
            mapOf(
                Pair("specversion", "0.3"),
                Pair("id", UUID.randomUUID().toString()),
                Pair("type", "$FLUENTD_PREFIX.mqtt.app.event"),
                Pair("source", "flink:${runtimeContext.taskNameWithSubtasks}/UDF/${javaClass.simpleName}"),
                Pair("time", CloudEventDateTimeFormatter.format(eventTime)),
                Pair("subject", "signpost"),
                Pair("message", "Disconnected from MQTT source($uri, $topic)")
            )
        )
    }

    override fun cancel() {
        interrupted.set(true)
        val eventTime = Instant.now()
        FLUENCY.emit(
            "$FLUENTD_PREFIX.mqtt.app.event",
            EventTime.fromEpoch(eventTime.epochSecond, eventTime.nano.toLong()),
            mapOf(
                Pair("specversion", "0.3"),
                Pair("id", UUID.randomUUID().toString()),
                Pair("type", "$FLUENTD_PREFIX.mqtt.app.event"),
                Pair("source", "flink:${runtimeContext.taskNameWithSubtasks}/UDF/${javaClass.simpleName}"),
                Pair("time", CloudEventDateTimeFormatter.format(eventTime)),
                Pair("subject", "signpost"),
                Pair("message", "MQTT Connection cancelled($uri, $topic)")
            )
        )
    }

    companion object {
        private val gson = Gson()
        private val typeToken = object : TypeToken<Map<String, Any>>() {}.type
    }
}