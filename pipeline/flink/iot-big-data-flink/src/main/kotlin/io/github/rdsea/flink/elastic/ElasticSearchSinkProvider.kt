package io.github.rdsea.flink.elastic

import io.github.rdsea.flink.domain.WindowedSensorReport
import io.github.rdsea.flink.util.Configuration
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
class ElasticSearchSinkProvider private constructor() {

    companion object {

        fun get(configuration: Configuration): ElasticsearchSink<WindowedSensorReport> {
            val elasticSinkBuilder = ElasticsearchSink.Builder<WindowedSensorReport>(listOf(HttpHost(configuration.elasticUri.host, configuration.elasticUri.port)), ElasticSearchInsertionSinkFunction())
            elasticSinkBuilder.setBulkFlushMaxActions(1)
            return elasticSinkBuilder.build()
        }
    }
}
