package io.github.rdsea.reasoner.sink

import io.github.rdsea.reasoner.domain.Incident
import java.net.URI
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class ElasticSearchSinkFactory private constructor() {

    companion object {
        fun create(elasticUri: URI): ElasticsearchSink<Incident> {
            val elasticSinkBuilder = ElasticsearchSink.Builder<Incident>(
                listOf(HttpHost(elasticUri.host, elasticUri.port)), IncidentElasticSearchSink()
            )
            elasticSinkBuilder.setBulkFlushMaxActions(1)
            return elasticSinkBuilder.build()
        }
    }
}
