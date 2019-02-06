package io.github.rdsea.flink

import io.github.rdsea.flink.domain.SensorRecord
import io.github.rdsea.flink.elastic.ElasticSearchInsertionSinkFunction
import io.github.rdsea.flink.mqtt.MqttMessageToSensorRecordMapper
import io.github.rdsea.flink.mqtt.MqttSource
import io.github.rdsea.flink.util.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import java.lang.IllegalArgumentException
import java.net.URI

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class Main {

    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val config = checkArgs(args)
            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            val dataStream = env
                .addSource(MqttSource(config.mqttUri, config.mqttTopic))
                .map(MqttMessageToSensorRecordMapper())

            // apply custom transformation on dataStream

            // send transformed data to Elasticsearch
            val elasticSinkBuilder = ElasticsearchSink.Builder<SensorRecord>(listOf(HttpHost(config.elasticUri.host, config.elasticUri.port)), ElasticSearchInsertionSinkFunction())
            elasticSinkBuilder.setBulkFlushMaxActions(1)

            val elasticsearchSink = elasticSinkBuilder.build()
            // dataStream.countWindowAll(7)

            dataStream.addSink(elasticsearchSink)

            env.execute("IoT Big Data Analytics Example")
        }

        private fun checkArgs(args: Array<String>): Configuration {
            if (args.size != 3) {
                throw IllegalArgumentException("There must be exactly 3 arguments: <MQTT_URL> <MQTT_TOPIC> <ELASTICSEARCH_URL>")
            }
            checkStringParam("MQTT_URL", args[0])
            checkStringParam("MQTT_TOPIC", args[1])
            checkStringParam("ELASTICSEARCH_URL", args[2])

            return Configuration(URI(args[0]), args[1], URI(args[2]))
        }

        private fun checkStringParam(param: String, str: String) {
            if (str.trim().isBlank()) {
                throw IllegalArgumentException("Parameter <$param> must not be blank")
            }
        }
    }
}