package io.github.rdsea.reasoner

import io.github.rdsea.reasoner.source.KafkaRecordMapFunction
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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
            checkArgs(args)

            val env = StreamExecutionEnvironment.getExecutionEnvironment()

            val properties = Properties()
            properties.setProperty("bootstrap.servers", args[0])
            properties.setProperty("group.id", args[1])
            env
                .addSource(FlinkKafkaConsumer("signals", SimpleStringSchema(), properties))
                .map(KafkaRecordMapFunction()).name("KafkaRecordMapFunction")
                .print()

            env.execute("Incident Reasoning")
        }

        private fun checkArgs(args: Array<String>) {
            if (args.size != 2) {
                throw IllegalArgumentException("There must be exactly 2 arguments: <BROKERS> <GROUP_ID>")
            }
        }
    }
}
