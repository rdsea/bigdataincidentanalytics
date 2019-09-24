package io.github.rdsea.flink.domain

import com.google.gson.annotations.SerializedName
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
data class SensorRecord(
    var id: String = "",
    @SerializedName("station_id")
    var stationId: String = "",
    @SerializedName("alarm_id")
    var alarmId: String = "",
    @SerializedName("parameter_id")
    var parameterId: String = "",
    @SerializedName("start_time")
    var startTime: LocalDateTime = LocalDateTime.MIN,
    @SerializedName("end_time")
    var endTime: LocalDateTime = LocalDateTime.MIN,
    @SerializedName("value")
    var sensorValue: Double = 0.0,
    var threshold: Double = 0.0
    // var prov: Provenance = Provenance()
)

data class Provenance(
    var id: String = "",
    var type: String = "",
    var wasDerivedFrom: String = "",
    var wasGeneratedBy: String = ""
) : Serializable