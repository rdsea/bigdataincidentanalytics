package io.github.rdsea.reasoner.domain

import java.time.LocalDateTime
import org.neo4j.driver.types.Node

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
) {
    companion object {
        fun fromNeoNode(node: Node): SignalNode {
            val threshold: Int? = getIntValueOrNull(node, "threshold")
            val thresholdCounter: Int? = getIntValueOrNull(node, "thresholdCounter")
            val coolDownSec: Int? = getIntValueOrNull(node, "coolDownSec")
            return SignalNode(node["name"].asString(), threshold, thresholdCounter,
                node["lastSignalTime"].asLocalDateTime(null), coolDownSec)
        }
    }
}

private fun getIntValueOrNull(node: Node, key: String): Int? {
    return if (node[key].isNull) {
        null
    } else {
        node[key].asInt()
    }
}
