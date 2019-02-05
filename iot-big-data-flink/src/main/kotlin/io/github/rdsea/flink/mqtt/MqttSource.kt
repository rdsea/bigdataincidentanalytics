package io.github.rdsea.flink.mqtt

import mu.KLogging
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.fusesource.mqtt.client.MQTT
import org.fusesource.mqtt.client.QoS
import org.fusesource.mqtt.client.Topic
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

    override fun run(ctx: SourceFunction.SourceContext<MqttMessage>) {
        interrupted.set(false)
        val mqtt = MQTT()
        mqtt.host = uri
        val blockingConnection = mqtt.blockingConnection()
        blockingConnection.connect()
        blockingConnection.subscribe(arrayOf(Topic(topic, QoS.AT_LEAST_ONCE)))

        while (blockingConnection.isConnected && !interrupted.get()) {
            val message = blockingConnection.receive()
            logger.info { "MQTT message received" }
            val mqttMessage = MqttMessage(message.topic, String(message.payload))
            message.ack()
            ctx.collect(mqttMessage)
        }

        blockingConnection.disconnect()
    }

    override fun cancel() {
        interrupted.set(true)
        logger.info { "Source cancelled" }
    }

    companion object : KLogging()
}