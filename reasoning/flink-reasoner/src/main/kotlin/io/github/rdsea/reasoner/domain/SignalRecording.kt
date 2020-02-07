package io.github.rdsea.reasoner.domain

import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.Table
import java.util.Date
import java.util.UUID

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
@Table(keyspace = "incident_analytics", name = "signals")
data class SignalRecording(
    @Column(name = "uuid")
    var uuid: UUID = UUID.randomUUID(),
    @Column(name = "timestamp")
    var timestamp: Date? = null,
    @Column(name = "signal_name")
    var name: String = "",
    @Column(name = "pipeline_component")
    var pipelineComponent: String = ""
)
