package io.github.rdsea.reasoner.sink

import io.github.rdsea.reasoner.Main
import io.github.rdsea.reasoner.domain.Incident
import org.apache.flink.api.common.functions.RuntimeContext
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
class IncidentElasticSearchSink : ElasticsearchSinkFunction<Incident> {

    override fun process(element: Incident, ctx: RuntimeContext, indexer: RequestIndexer) {
        val json = Main.gson.toJson(element)
        val request = Requests.indexRequest()
            .index("iot-incident-reports")
            .type("") // empty string workaround: starting with 7.0 Elasticsearch doesn't support "type" in bulk requests
            .source(json, XContentType.JSON)
        indexer.add(request)
    }
}
