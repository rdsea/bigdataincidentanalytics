package io.github.rdsea.reasoner.domain

import com.google.gson.annotations.JsonAdapter
import io.github.rdsea.reasoner.util.SignalDetailsDeserializer
import java.sql.Timestamp
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
    var timestamp: LocalDateTime? = null,
    var pipelineComponent: String = "",
    var summary: String = "",
    @JsonAdapter(SignalDetailsDeserializer::class)
    var details: Map<String, Any>? = null,
    var threshold: Int = -1,
    var thresholdCounter: Int = -1,
    var lastSignalTime: LocalDateTime? = null,
    var coolDownSec: Int = -1
)

enum class SignalType {
    LOG,
    PROMETHEUS_ALERT
}

fun Signal.toRecording(): SignalRecording {
    return SignalRecording(
        timestamp = Timestamp.valueOf(this.timestamp!!),
        name = this.name,
        pipelineComponent = this.pipelineComponent
    )
}
