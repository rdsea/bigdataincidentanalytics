package io.github.rdsea.flink.elastic

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.github.rdsea.flink.FLUENCY
import io.github.rdsea.flink.FLUENTD_PREFIX
import io.github.rdsea.flink.domain.WindowedSensorReport
import io.github.rdsea.flink.util.CloudEventDateTimeFormatter
import io.github.rdsea.flink.util.LocalDateTimeJsonSerializer
import java.time.Instant
import java.time.LocalDateTime
import java.util.UUID
import mu.KLogging
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.komamitsu.fluency.EventTime

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class ElasticSearchInsertionSinkFunction : ElasticsearchSinkFunction<WindowedSensorReport> {

    override fun process(element: WindowedSensorReport, ctx: RuntimeContext, indexer: RequestIndexer) {
        val indexRequest = createIndexRequest(element)
        logger.debug { "SINK - new IndexRequest created" }
        indexer.add(indexRequest)

        val instant = Instant.now()
        FLUENCY.emit(
            "$FLUENTD_PREFIX.storage.app.dataAsset",
            EventTime.fromEpoch(instant.epochSecond, instant.nano.toLong()),
            mapOf(
                Pair("specversion", "0.3"),
                Pair("id", UUID.randomUUID().toString()),
                Pair("type", "$FLUENTD_PREFIX.storage.app.dataAsset"),
                Pair("source", "flink:${ctx.taskNameWithSubtasks}/${javaClass.simpleName}"),
                Pair("time", CloudEventDateTimeFormatter.format(instant)),
                Pair("subject", element.id),
                Pair("message", "Sending windowed sensor report of device ${element.deviceId} to data store"),
                Pair("data", element)
            )
        )
    }

    private fun createIndexRequest(element: WindowedSensorReport): IndexRequest {
        val json: Map<String, Any> = gson.fromJson(gson.toJson(element), typeToken)

        return Requests.indexRequest()
            .index("flink-sensor-data")
            .type("") // empty string workaround: starting with 7.0 Elasticsearch doesn't support "type" in bulk requests
            .source(json)
    }

    companion object : KLogging() {
        private val gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()
        private val typeToken = object : TypeToken<Map<String, Any>>() {}.type
    }
}
