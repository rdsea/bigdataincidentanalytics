package io.github.rdsea.reasoner.domain

import com.google.gson.annotations.JsonAdapter
import io.github.rdsea.reasoner.util.SignalDetailsDeserializer
import java.sql.Timestamp
import java.time.Duration
import java.time.LocalDateTime

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class Signal(
    var type: SignalType? = null,
    var name: String = "",
    var timestamp: LocalDateTime = LocalDateTime.now(),
    var pipelineComponent: String = "",
    var summary: String = "",
    @JsonAdapter(SignalDetailsDeserializer::class)
    var details: Map<String, Any>? = null,
    var threshold: Int = -1,
    var thresholdCounter: Int = -1,
    var lastSignalTime: LocalDateTime? = null,
    var coolDownSec: Int = -1
) {

    fun requiresMultipleOccurrences(): Boolean {
        return threshold > 0 && coolDownSec > 0
    }

    fun isWithinCoolDownWindow(): Boolean {
        return this.lastSignalTime == null || Duration.between(lastSignalTime, timestamp).abs().seconds < coolDownSec
    }

    fun isCounterInitialized(): Boolean {
        return thresholdCounter >= 0
    }

    /**
     * Returns true if this signal is fired.
     *
     * For signals that require multiple occurrences within a time window, this means that the counter
     * reached/exceeded the threshold.
     *
     * For "simple" signals this is trivially true.
     */
    fun isActivated(): Boolean {
        return thresholdCounter >= threshold
    }

    fun toRecording(): SignalRecording {
        return SignalRecording(
            timestamp = Timestamp.valueOf(this.timestamp),
            name = this.name,
            pipelineComponent = this.pipelineComponent
        )
    }
}

enum class SignalType {
    LOG,
    PROMETHEUS_ALERT
}
