package io.github.rdsea.reasoner

import io.github.rdsea.reasoner.dao.Neo4jDAO
import io.github.rdsea.reasoner.process.SignalProcessFunction
import io.github.rdsea.reasoner.sink.ElasticSearchSinkFactory
import io.github.rdsea.reasoner.source.KafkaRecordMapFunction
import java.net.URI
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase

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
        private val driver: Driver = GraphDatabase.driver("bolt://neo4j:7687", AuthTokens.basic("neo4j", "test"))

        @JvmStatic fun main(args: Array<String>) {
            checkArgs(args)
            val properties = getKafkaProperties(args)

            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            env
                .addSource(FlinkKafkaConsumer(properties.getProperty("topic"), SimpleStringSchema(), properties))
                .name("KafkaSource")
                .map(KafkaRecordMapFunction()).name("KafkaRecordMapFunction")
                .process(SignalProcessFunction(Neo4jDAO())).name("SignalReasoningProcessFunction")
                .addSink(ElasticSearchSinkFactory.create(URI(args[3]))).name("ElasticSearchSink")

            env.execute("Incident Reasoning")
        }

        fun getNeo4jDriver(): Driver {
            return driver
        }

        private fun checkArgs(args: Array<String>) {
            if (args.size != 4) {
                throw IllegalArgumentException("There must be exactly 4 arguments: <BROKERS> <GROUP_ID> <TOPIC> <ELASTICSEARCH_URI>")
            }
        }

        private fun getKafkaProperties(args: Array<String>): Properties {
            val properties = Properties()
            properties.setProperty("bootstrap.servers", args[0])
            properties.setProperty("group.id", args[1])
            properties.setProperty("topic", args[2])
            return properties
        }
    }
}
