package io.github.rdsea.reasoner.source

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.functions.MapFunction

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class KafkaRecordMapFunction : MapFunction<String, Map<String, Any>> {

    override fun map(value: String): Map<String, Any> {
        return gson.fromJson(value, typeToken)
    }

    companion object {
        private val typeToken = object : TypeToken<Map<String, Any>>() {}.type
        private val gson = Gson()
    }
}
