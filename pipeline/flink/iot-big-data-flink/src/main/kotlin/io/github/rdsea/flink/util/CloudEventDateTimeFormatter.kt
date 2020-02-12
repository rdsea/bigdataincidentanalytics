package io.github.rdsea.flink.util

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class CloudEventDateTimeFormatter private constructor() {
    companion object {
        fun format(instant: Instant): String {
            return ZonedDateTime
                .ofInstant(instant, ZoneId.of("Z"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        }
    }
}
