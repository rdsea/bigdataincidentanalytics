package io.github.rdsea.cep

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.lang.IllegalArgumentException
import java.util.Properties

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
        private val gson = Gson()
        private val typeToken = object : TypeToken<Map<String, Any>>() {}.type

        @JvmStatic fun main(args: Array<String>) {
            checkArgs(args)

            val env = StreamExecutionEnvironment.getExecutionEnvironment()

            val properties = Properties()
            properties.setProperty("bootstrap.servers", args[0])
            properties.setProperty("group.id", args[1])
            val dataStream: DataStream<Map<String, Any>> = env
                .addSource(FlinkKafkaConsumer("signals", SimpleStringSchema(), properties))
                .map {
                    val json: Map<String, Any> = gson.fromJson(it, typeToken)
                    json
                }

            env.execute("Complex Event Processing")
        }

        private fun checkArgs(args: Array<String>) {
            if (args.size != 2) {
                throw IllegalArgumentException("There must be exactly 2 arguments: <BROKERS> <GROUP_ID>")
            }
        }
    }
}