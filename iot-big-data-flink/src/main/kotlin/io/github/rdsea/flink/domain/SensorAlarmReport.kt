package io.github.rdsea.flink.domain

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
data class SensorAlarmReport(
    val stationId: String,
    val numberOfAlarms: Int,
    val averageSensorValue: Double
)