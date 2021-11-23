package simpledb.tx.recovery

import simpledb.buffer.BufferManager
import simpledb.log.LogManager

class RecoveryManager(
    private val transaction: Transaction,
    private val transactionNumber: Int,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {
    init {
        StartRecord.writeTolog(logManager, transactionNumber)
    }


}