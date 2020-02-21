package io.github.rdsea.reasoner.util

import com.google.gson.TypeAdapter
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.gson.stream.JsonWriter
import io.github.rdsea.reasoner.Main
import java.lang.reflect.Type

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class SignalDetailsDeserializer : TypeAdapter<Map<String, Any>?>() {

    private val genericMapType: Type by lazy { object : TypeToken<Map<String, Any>>() {}.type }

    override fun read(input: JsonReader): Map<String, Any>? {
        if (input.peek() == JsonToken.NULL) return null
        val json = input.nextString()
        return Main.gson.fromJson(json, genericMapType)
    }

    override fun write(out: JsonWriter, value: Map<String, Any>?) {
        if (value == null) {
            out.nullValue()
        } else {
            out.jsonValue(Main.gson.toJson(value))
        }
    }
}
