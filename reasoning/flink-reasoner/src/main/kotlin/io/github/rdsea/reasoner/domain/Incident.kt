package io.github.rdsea.reasoner.domain

import java.io.Serializable
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
data class Incident(
    var name: String,
    var dateTime: LocalDateTime,
    var compositeSignal: CompositeSignal
) : Serializable {

    private val summary = "Incident \"${this.name}\" is active! " +
        "It is indicated by ${this.compositeSignal.activeSignalsSorted.size} signal(s)."
    private val participatingComponents = compositeSignal.activeSignalsSorted
        .map { it.pipelineComponent }
        .distinct()

    companion object {
        const val RE_NOTIFICATION_WAIT_SEC = 120 // for this amount of seconds the same Incident won't be reported twice
        private const val serialVersionUID = 20180617104402L
    }
}

data class IncidentEntity(
    var name: String
)
