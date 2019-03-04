package io.github.rdsea.flink.mqtt

import java.io.Serializable

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class MqttMessage(
    var topic: String = "",
    var payload: String = ""
) : Serializable {

    companion object {
        private const val serialVersionUID = 20180617104400L
    }
}