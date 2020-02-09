package io.github.rdsea.reasoner.dao

import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.IncidentEntity
import io.github.rdsea.reasoner.domain.Signal
import java.util.Optional

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
interface DAO {

    fun initialize()

    fun findSignal(signal: Signal): Optional<Signal>

    fun updateSignalAndGetActivatedCompositeSignals(signal: Signal): List<CompositeSignal>

    fun findIncidentsOfCompositeSignal(compositeSignal: CompositeSignal): List<IncidentEntity>

    fun tearDown()
}
