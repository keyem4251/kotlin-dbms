package simpledb.tx.recovery

import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.log.LogManager

class RecoveryManager(
    private val transaction: Transaction,
    private val transactionNumber: Int,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {
    init {
        StartRecord.writeToLog(logManager, transactionNumber)
    }

    fun commit() {
        bufferManager.flushAll(transactionNumber)
        val lsn = CommitRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    fun rollback() {
        doRollback()
        bufferManager.flushAll(transactionNumber)
        val lsn = RollbackRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    fun recover() {
        doRecover()
        bufferManager.flushAll(transactionNumber)
        val lsn = CheckpointRecord.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun setInt(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getInt(offset)
        val blockId = buffer.blockId()
        return SetIntRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
    }

    fun setString(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getString(offset)
        val blockId = buffer.blockId()
        return SetStringRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
    }

    private fun doRollback() {
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes)
            if (record != null && record.txNumber() == transactionNumber) {
                if (record.op() == Operator.START.id) return
                record.undo(transaction)
            }
        }
    }

    private fun doRecover() {
        val finishedTransaction = mutableListOf<Int>()
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes) ?: return
            if (record.op() == Operator.CHECKPOINT.id) return

            if (record.op() == Operator.COMMIT.id || record.op() == Operator.ROLLBACK.id) {
                finishedTransaction.add(record.txNumber())
            } else if (!finishedTransaction.contains(record.txNumber())) {
                record.undo(transaction)
            }
        }
    }
}
