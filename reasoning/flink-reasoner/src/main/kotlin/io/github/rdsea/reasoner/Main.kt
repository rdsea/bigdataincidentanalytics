package io.github.rdsea.reasoner

import com.datastax.driver.mapping.Mapper
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.github.rdsea.reasoner.dao.Neo4jDAO
import io.github.rdsea.reasoner.domain.Signal
import io.github.rdsea.reasoner.process.CompositeSignalProcessFunction
import io.github.rdsea.reasoner.process.SignalProcessFunction
import io.github.rdsea.reasoner.sink.ElasticSearchSinkFactory
import io.github.rdsea.reasoner.source.KafkaRecordMapFunction
import io.github.rdsea.reasoner.source.SignalToRecordingMapFunction
import io.github.rdsea.reasoner.util.LocalDateTimeJsonSerializer
import java.lang.reflect.Type
import java.net.URI
import java.time.LocalDateTime
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.OutputTag
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
        val gson: Gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()
        val genericMapType: Type = object : TypeToken<Map<String, Any>>() {}.type

        @JvmStatic fun main(args: Array<String>) {
            checkArgs(args)
            val properties = getKafkaProperties(args)

            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            val outPutTag = object : OutputTag<Signal>("side-output") {}
            val stream = env
                .addSource(FlinkKafkaConsumer(properties.getProperty("topic"), SimpleStringSchema(), properties))
                .name("KafkaSource")
                .map(KafkaRecordMapFunction()).name("KafkaRecordMapFunction")
                .process(SignalProcessFunction(Neo4jDAO())).name("SignalProcessFunction")
                .process(CompositeSignalProcessFunction(Neo4jDAO())).name("CompositeSignalProcessFunction")

            // Incidents generated by the ProcessFunction are stored in ElasticSearch
            stream.addSink(ElasticSearchSinkFactory.create(URI(args[3]))).name("ElasticSearchSink")

            // ReducedSignals from the Side Output of the ProcessFunction are stored in Cassandra
            val sideOutput = stream
                .getSideOutput(outPutTag)
                .map(SignalToRecordingMapFunction()).name("SignalToRecordingMapFunction")

            CassandraSink.addSink(sideOutput)
                .setHost("cassandra")
                .setMapperOptions { arrayOf<Mapper.Option>(Mapper.Option.saveNullFields(false)) }
                .build().name("CassandraSink")

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
