package io.github.rdsea.analysis

import java.io.Serializable
import java.sql.Timestamp
import java.time.Instant

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class SignalRecord(
    var uuid: String = "",
    var timestamp: Timestamp = defaultDate,
    var name: String = "",
    var pipelineComponent: String = ""
) : Serializable {
    companion object {
        private val defaultDate = Timestamp.from(Instant.now())
    }
}
