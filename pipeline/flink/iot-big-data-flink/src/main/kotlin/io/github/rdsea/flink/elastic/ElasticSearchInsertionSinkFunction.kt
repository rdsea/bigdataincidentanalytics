package io.github.rdsea.flink.elastic

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.github.rdsea.flink.FLUENCY
import io.github.rdsea.flink.FLUENTD_PREFIX
import io.github.rdsea.flink.domain.Provenance
import io.github.rdsea.flink.domain.SensorAlarmReport
import io.github.rdsea.flink.domain.withProv
import io.github.rdsea.flink.util.LocalDateTimeJsonSerializer
import mu.KLogging
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.komamitsu.fluency.EventTime
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
        val reportWithProv = element.withProv(createDataProvenance(element))
        val indexRequest = createIndexRequest(reportWithProv)
        logger.debug { "SINK - new IndexRequest created" }
        indexer.add(indexRequest)
        FLUENCY.emit("$FLUENTD_PREFIX.storage",
            EventTime.fromEpochMilli(System.currentTimeMillis()),
            mapOf(
                Pair("log", "Sending sensor alarm report of station ${reportWithProv.stationId} to data store"),
                Pair("layer", "APPLICATION"),
                Pair("data", reportWithProv)
            )
        )
    }

    private fun createIndexRequest(element: SensorAlarmReport): IndexRequest {
        val json: Map<String, Any> = gson.fromJson(gson.toJson(element), typeToken)

        return Requests.indexRequest()
            .index("flink-sensor-data")
            .type("sensor")
            .source(json)
    }

    private fun createDataProvenance(element: SensorAlarmReport): Provenance {
        return Provenance(
            previousId = "flink-${javaClass.simpleName}",
            type = "sensorDataReport",
            wasDerivedFrom = element.prov.previousId,
            wasGeneratedBy = "flink-${javaClass.simpleName}"
        )
    }

    companion object : KLogging() {
        private val gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()
        private val typeToken = object : TypeToken<Map<String, Any>>() {}.type
    }
}