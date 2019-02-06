package io.github.rdsea.flink.domain

import com.google.gson.annotations.SerializedName
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
data class SensorRecord(
    val id: String,
    @SerializedName("station_id")
    val stationId: String,
    @SerializedName("alarm_id")
    val alarmId: String,
    @SerializedName("parameter_id")
    val parameterId: String,
    @SerializedName("start_time")
    val startTime: LocalDateTime,
    @SerializedName("end_time")
    val endTime: LocalDateTime,
    @SerializedName("value")
    val sensorValue: Double,
    val threshold: Double
)