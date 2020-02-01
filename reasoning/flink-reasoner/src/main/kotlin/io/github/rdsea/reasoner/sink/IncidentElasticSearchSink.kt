package io.github.rdsea.reasoner.sink

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.github.rdsea.reasoner.domain.Incident
import io.github.rdsea.reasoner.util.LocalDateTimeJsonSerializer
import java.time.LocalDateTime
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.client.Requests

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class IncidentElasticSearchSink : ElasticsearchSinkFunction<Incident> {

    override fun process(element: Incident, ctx: RuntimeContext, indexer: RequestIndexer) {
        val json: MutableMap<String, Any> = gson.fromJson(gson.toJson(element), typeToken)
        json["summary"] = "Incident \"${element.name}\" is active! It is indicated by ${element.activatedSignals.size} signal(s)."
        val request = Requests.indexRequest()
            .index("iot-incidents")
            .type("") // empty string workaround: starting with 7.0 Elasticsearch doesn't support "type" in bulk requests
            .source(json)
        indexer.add(request)
    }

    companion object {
        private val gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()
        private val typeToken = object : TypeToken<MutableMap<String, Any>>() {}.type
    }
}
