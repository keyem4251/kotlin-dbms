package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

class StartRecord(val page: Page): LogRecord {
    private var transactionNumber: Int

    init {
        val transactionPosition = Integer.BYTES
        transactionNumber = page.getInt(transactionPosition)
    }

    override fun op(): Int {
        return Operator.START.id
    }

    override fun txNumber(): Int {
        return transactionNumber
    }

    override fun undo(transaction: Transaction) {}

    override fun toString(): String {
        return "<START $transactionNumber>"
    }

    companion object {
        fun writeToLog(logManager: LogManager, transactionNumber: Int): Int {
            val record = ByteArray(2 * Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.START.id)
            page.setInt(Integer.BYTES, transactionNumber)
            return logManager.append(record)
        }
    }
}