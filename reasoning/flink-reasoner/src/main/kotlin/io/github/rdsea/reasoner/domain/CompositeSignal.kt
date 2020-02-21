package io.github.rdsea.reasoner.domain

import com.google.gson.annotations.SerializedName

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
    var coolDownSec: Int = DEFAULT_COOL_DOWN_SEC,
    var activationThreshold: Double = DEFAULT_ACTIVATION_THRESHOLD,
    var numOfConnectedSignals: Int = 1,
    @SerializedName("activeSignals")
    var activeSignalsSorted: List<Signal> = emptyList()
) {
    companion object {
        const val DEFAULT_ACTIVATION_THRESHOLD = 1.0 // default is 100%, i.e. all connected signals must fire
        const val DEFAULT_COOL_DOWN_SEC = 60 // default is 1 minute
    }
}
