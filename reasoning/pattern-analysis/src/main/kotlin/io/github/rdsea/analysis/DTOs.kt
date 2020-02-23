package io.github.rdsea.analysis

import java.io.Serializable
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime

/**
 * Data class mirroring the model coming from the external
 * source (in our case Cassandra).
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

/**
 * Object representing an item that is used in the Frequent-Pattern Matching
 * algorithm.
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class Item(
    var signalId: String = "",
    var timestamp: Timestamp = defaultTimestamp
) : Serializable {
    companion object {
        private val defaultTimestamp = Timestamp.from(Instant.now())
    }
}

/**
 * Object encapsulating a prediction result.
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class Prediction(
    val analysisId: String,
    val analysisTimestamp: LocalDateTime,
    val signals: List<String>,
    val predictedSignals: List<String>
) : Serializable
