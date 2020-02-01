package io.github.rdsea.reasoner.dao

import io.github.rdsea.reasoner.domain.CompositeSignal
import io.github.rdsea.reasoner.domain.SignalNode
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

    fun readSignalByName(signalName: String): Optional<SignalNode>

    fun updateSignal(signal: SignalNode)

    fun readCompositeSignalsOfSignalByName(signalName: String): List<CompositeSignal>

    fun updateCompositeSignal(compositeSignal: CompositeSignal)

    fun tearDown()
}
