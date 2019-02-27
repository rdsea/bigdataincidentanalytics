package io.github.rdsea.flink.mqtt

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.github.rdsea.flink.FLUENCY
import io.github.rdsea.flink.FLUENTD_PREFIX
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.fusesource.mqtt.client.MQTT
import org.fusesource.mqtt.client.QoS
import org.fusesource.mqtt.client.Topic
import org.komamitsu.fluency.EventTime
import java.net.URI
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
        FLUENCY.emit("$FLUENTD_PREFIX.mqtt", mapOf(Pair("message", "connected to MQTT source")))
        while (blockingConnection.isConnected && !interrupted.get()) {
            val message = blockingConnection.receive()
            val mqttMessage = MqttMessage(message.topic, String(message.payload))
            val json: Map<String, String> = gson.fromJson(mqttMessage.payload, typeToken)
            FLUENCY.emit("$FLUENTD_PREFIX.mqtt",
                EventTime.fromEpochMilli(System.currentTimeMillis()),
                mapOf(
                    Pair("message", "MQTT message received"),
                    Pair("stage", "input"),
                    Pair("dataId", json["id"]),
                    Pair("topic", mqttMessage.topic),
                    Pair("payload", json)
                )
            )
            message.ack()
            ctx.collect(mqttMessage)
        }

        blockingConnection.disconnect()
        FLUENCY.emit("$FLUENTD_PREFIX.mqtt", mapOf(Pair("message", "Disconnected from MQTT source")))
    }

    override fun cancel() {
        interrupted.set(true)
        FLUENCY.emit("$FLUENTD_PREFIX.mqtt", mapOf(Pair("message", "Connection cancelled")))
    }

    companion object {
        private val gson = Gson()
        private val typeToken = object : TypeToken<Map<String, String>>() {}.type
    }
}