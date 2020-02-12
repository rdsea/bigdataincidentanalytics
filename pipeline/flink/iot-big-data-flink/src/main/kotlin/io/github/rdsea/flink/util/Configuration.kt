package io.github.rdsea.flink.util

import java.net.URI

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class Configuration(
    val mqttUri: URI,
    val mqttTopic: String,
    val elasticUri: URI
)
