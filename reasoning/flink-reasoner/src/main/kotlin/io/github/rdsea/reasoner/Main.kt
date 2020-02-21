package io.github.rdsea.reasoner

import com.datastax.driver.mapping.Mapper
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.github.rdsea.reasoner.dao.Neo4jDAO
import io.github.rdsea.reasoner.domain.Signal
import io.github.rdsea.reasoner.process.CompositeSignalProcessFunction
import io.github.rdsea.reasoner.process.SignalProcessFunction
import io.github.rdsea.reasoner.sink.ElasticSearchSinkFactory
import io.github.rdsea.reasoner.source.KafkaRecordMapFunction
import io.github.rdsea.reasoner.source.SignalToRecordingMapFunction
import io.github.rdsea.reasoner.util.LocalDateTimeJsonSerializer
import java.net.URI
import java.time.LocalDateTime
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.OutputTag

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
            val properties = getKafkaProperties(args)

            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            val outPutTag = object : OutputTag<Signal>("side-output") {}

            // This main stream consumes incoming signals from Kafka, processes them:
            //  (1) if signal is activating CompositeSignals, then these are forwarded for further processing
            //  (2) each signal is emitted through a side output no matter its significance in a reduced form
            val stream = env
                .addSource(FlinkKafkaConsumer(properties.getProperty("topic"), SimpleStringSchema(), properties))
                .name("KafkaSource")
                .map(KafkaRecordMapFunction()).name("KafkaRecordMapFunction")
                .process(SignalProcessFunction(Neo4jDAO())).name("SignalProcessFunction")

            // CompositeSignals are processed and potentially fired Incidents stored in ElasticSearch
            stream
                .process(CompositeSignalProcessFunction(Neo4jDAO())).name("CompositeSignalProcessFunction")
                .addSink(ElasticSearchSinkFactory.create(URI(args[3]))).name("ElasticSearchSink")

            // ReducedSignals from the Side Output of the main stream are stored in Cassandra
            val sideOutput = stream
                .getSideOutput(outPutTag)
                .map(SignalToRecordingMapFunction()).name("SignalToRecordingMapFunction")

            CassandraSink.addSink(sideOutput)
                .setHost(args[4])
                .setMapperOptions { arrayOf<Mapper.Option>(Mapper.Option.saveNullFields(false)) }
                .build().name("CassandraSink")

            env.execute("Incident Reasoning")
        }

        // Keep a single, reusable instance of Gson
        val gson: Gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()

        private fun checkArgs(args: Array<String>) {
            if (args.size != 5) {
                throw IllegalArgumentException("There must be exactly 5 arguments: <BROKERS> <GROUP_ID> <TOPIC> <ELASTICSEARCH_URI> <CASSANDRA_HOST>")
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
