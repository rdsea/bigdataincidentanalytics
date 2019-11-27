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
data class WindowedSensorReport(
    val id: String,
    val deviceId: String,
    val numberOfMeasurements: Int,
    val averageHumidity: Double,
    val averageTemperature: Double,
    val measurementIds: List<String>
)
