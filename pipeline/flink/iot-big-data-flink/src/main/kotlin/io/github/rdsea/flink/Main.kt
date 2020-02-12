package io.github.rdsea.flink

import io.github.rdsea.flink.elastic.ElasticSearchSinkProvider
import io.github.rdsea.flink.mqtt.MqttMessageToSensorRecordMapper
import io.github.rdsea.flink.mqtt.MqttSource
import io.github.rdsea.flink.processing.SensorDataWindowFunction
import io.github.rdsea.flink.processing.SensorRecordKeySelector
import io.github.rdsea.flink.util.Configuration
import java.lang.IllegalArgumentException
import java.net.URI
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.komamitsu.fluency.Fluency
import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
val FLUENTD_PREFIX = System.getenv("FLUENTD_TAG_PREFIX") ?: "flink-processing"
val FLUENCY: Fluency = FluencyBuilderForFluentd()
    .build(System.getenv("FLUENTD_HOST") ?: "localhost", System.getenv("FLUENTD_PORT")?.toInt() ?: 24224)
class Main {

    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val config = checkArgs(args)
            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            env
                .addSource(MqttSource(config.mqttUri, config.mqttTopic), "MqttSource") // read from MQTT
                .map(MqttMessageToSensorRecordMapper()).name("MqttMessageToSensorRecordMap") // map to POJOs
                .keyBy(SensorRecordKeySelector()) // group records by "stationId"
                .countWindow(10) // collect 10 records per group
                .apply(SensorDataWindowFunction()).name("StationAlarmAggregationFunction") // execute analytics for each group of 3 records
                .addSink(ElasticSearchSinkProvider.get(config)).name("ElasticsearchSink") // send them to ElasticSearch

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
