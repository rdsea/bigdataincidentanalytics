package io.github.rdsea.flink.processing

import io.github.rdsea.flink.domain.SensorRecord
import org.apache.flink.api.java.functions.KeySelector

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class SensorRecordKeySelector : KeySelector<SensorRecord, String> {

    override fun getKey(value: SensorRecord): String {
        return value.deviceId
    }
}