package io.github.rdsea.reasoner.domain

import java.time.LocalDateTime
import org.neo4j.driver.Record
import org.neo4j.driver.Value

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
) {
    companion object {
        fun fromNeoRecord(record: Record): CompositeSignal {
            val numOfConnectedSignals = record["cnt"].asInt()
            val node = record.get("n").asNode()
            val signals =
                node.get("activeSignals").asList({ t: Value? -> t!!.asString() }, null)?.toMutableList()
            return CompositeSignal(node.get("name").asString(), node.get("coolDownSec").asInt(3600),
                node.get("activationThreshold").asDouble(1.0), numOfConnectedSignals,
                node.get("lastSignalTime").asLocalDateTime(null), signals)
        }
    }
}
