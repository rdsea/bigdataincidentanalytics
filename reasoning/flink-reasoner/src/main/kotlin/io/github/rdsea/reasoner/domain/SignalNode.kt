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
data class SignalNode(
    var name: String = "",
    var threshold: Int? = null,
    var thresholdCounter: Int? = null,
    var lastSignalTime: LocalDateTime? = null,
    var coolDownSec: Int? = null
)
