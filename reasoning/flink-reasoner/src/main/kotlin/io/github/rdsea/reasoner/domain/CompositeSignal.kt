package io.github.rdsea.reasoner.domain

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
data class CompositeSignal(
    var name: String = "",
    var coolDownSec: Int = 3600, // set default to 1 hour
    var activationThreshold: Double = 1.0,
    var numOfConnectedSignals: Int = 1,
    var lastSignalTime: LocalDateTime? = null,
    var activeSignals: MutableList<String>? = null
)