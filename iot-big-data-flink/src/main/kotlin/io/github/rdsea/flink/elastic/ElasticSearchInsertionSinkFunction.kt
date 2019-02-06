package io.github.rdsea.flink.elastic

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.github.rdsea.flink.domain.SensorAlarmReport
import io.github.rdsea.flink.util.LocalDateTimeJsonSerializer
import mu.KLogging
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import java.time.LocalDateTime

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class ElasticSearchInsertionSinkFunction : ElasticsearchSinkFunction<SensorAlarmReport> {

    override fun process(element: SensorAlarmReport, ctx: RuntimeContext, indexer: RequestIndexer) {
        val indexRequest = createIndexRequest(element)
        logger.debug { "SINK - new IndexRequest created" }
        indexer.add(indexRequest)
    }

    private fun createIndexRequest(element: SensorAlarmReport): IndexRequest {
        val type = object : TypeToken<Map<String, String>>() {}.type
        val json: Map<String, String> = gson.fromJson(gson.toJson(element), type)

        return Requests.indexRequest()
            .index("flink-sensor-data")
            .type("sensor")
            .source(json)
    }

    companion object : KLogging() {
        private val gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()
    }
}