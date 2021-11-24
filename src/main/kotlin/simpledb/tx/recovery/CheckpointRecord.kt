package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

class CheckpointRecord: LogRecord {
    override fun op(): Int {
        return Operator.CHECKPOINT.id
    }

    override fun txNumber(): Int {
        return -1 // dummy value
    }

    override fun undo(transaction: Transaction) {}

    override fun toString(): String {
        return "<CHECKPOINT>"
    }

    companion object {
        fun writeToLog(logManager: LogManager): Int {
            val record = ByteArray(Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.CHECKPOINT.id)
            return logManager.append(record)
        }
    }
}