package io.github.rdsea.reasoner.sink

import com.google.gson.Gson
import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.Incident
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class IncidentElasticSearchSink : ElasticsearchSinkFunction<Incident>, AbstractRichFunction() {

    private lateinit var gson: Gson

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        gson = Main.gson
    }

    override fun process(element: Incident, ctx: RuntimeContext, indexer: RequestIndexer) {
        val json = gson.toJson(element)
        val request = Requests.indexRequest()
            .index("iot-incidents")
            .type("") // empty string workaround: starting with 7.0 Elasticsearch doesn't support "type" in bulk requests
            .source(json, XContentType.JSON)
        indexer.add(request)
    }
}
