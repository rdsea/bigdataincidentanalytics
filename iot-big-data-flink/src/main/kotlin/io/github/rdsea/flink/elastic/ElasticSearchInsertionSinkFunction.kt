package io.github.rdsea.flink.elastic

import io.github.rdsea.flink.mqtt.MqttMessage
import mu.KLogging
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
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
class ElasticSearchInsertionSinkFunction : ElasticsearchSinkFunction<MqttMessage> {

    override fun process(element: MqttMessage, ctx: RuntimeContext, indexer: RequestIndexer) {
        val indexRequest = createIndexRequest(element)
        logger.info { "SINK - new IndexRequest created" }
        indexer.add(indexRequest)
    }

    private fun createIndexRequest(element: MqttMessage): IndexRequest {
        val json = mutableMapOf(Pair("data", element.payload))

        return Requests.indexRequest()
            .index("flink-sensor-data")
            .type("sensor")
            .source(json)
    }

    companion object : KLogging()
}