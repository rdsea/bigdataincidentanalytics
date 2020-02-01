package io.github.rdsea.reasoner

import io.github.rdsea.reasoner.process.SignalProcessFunction
import io.github.rdsea.reasoner.source.KafkaRecordMapFunction
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.slf4j.LoggerFactory

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
        private val log = LoggerFactory.getLogger(Main::class.java)
        val driver: Driver = GraphDatabase.driver("bolt://neo4j:7687", AuthTokens.basic("neo4j", "test"))

        @JvmStatic fun main(args: Array<String>) {
            checkArgs(args)
            // driver = GraphDatabase.driver("bolt://neo4j:7687", AuthTokens.basic("neo4j", "test"))

            val env = StreamExecutionEnvironment.getExecutionEnvironment()

            val properties = Properties()
            properties.setProperty("bootstrap.servers", args[0])
            properties.setProperty("group.id", args[1])
            env
                .addSource(FlinkKafkaConsumer("signals", SimpleStringSchema(), properties))
                .map(KafkaRecordMapFunction()).name("KafkaRecordMapFunction")
                .process(SignalProcessFunction())
                .print()

            env.execute("Incident Reasoning")
            driver.close()
            println("IM HERE!!!!!!")
            // TODO DB Drive cleanup needed
        }

        private fun checkArgs(args: Array<String>) {
            if (args.size != 5) {
                throw IllegalArgumentException("There must be exactly 5 arguments: <BROKERS> <GROUP_ID> <NEO4J_URI> <NEO4J_USER> <NEO4J_PASS>")
            }
        }
    }
}
