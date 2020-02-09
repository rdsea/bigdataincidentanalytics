package io.github.rdsea.reasoner.source

import io.github.rdsea.reasoner.domain.Signal
import io.github.rdsea.reasoner.domain.SignalRecording
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
class SignalToRecordingMapFunction : MapFunction<Signal, SignalRecording> {

    override fun map(value: Signal): SignalRecording {
        return value.toRecording()
    }
}
