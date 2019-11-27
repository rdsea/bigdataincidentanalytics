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
    @SerializedName("device_id")
    var deviceId: String = "",
    @SerializedName("humidity")
    var humidity: Int = Int.MAX_VALUE,
    @SerializedName("temperature")
    var temperature: Int = Int.MAX_VALUE,
    @SerializedName("time")
    var time: LocalDateTime = LocalDateTime.MIN
)
