package io.github.rdsea.flink.mqtt

import com.google.gson.GsonBuilder
import io.github.rdsea.flink.domain.SensorRecord
import io.github.rdsea.flink.util.LocalDateTimeJsonSerializer
import org.apache.flink.api.common.functions.MapFunction
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
class MqttMessageToSensorRecordMapper : MapFunction<MqttMessage, SensorRecord> {

    override fun map(value: MqttMessage): SensorRecord {
        val json = value.payload
        return gson.fromJson(json, SensorRecord::class.java)
    }

    companion object {
        private val gson = GsonBuilder().registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer()).create()
    }
}