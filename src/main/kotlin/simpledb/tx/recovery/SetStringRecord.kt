package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

class SetStringRecord(val page: Page): LogRecord {
    private var transactionNumber: Int
    private var offset: Int
    private var value: String
    private var blockId: BlockId

    init {
        val transactionPosition = Integer.BYTES
        transactionNumber = page.getInt(transactionPosition)
        val filePosition = transactionPosition + Integer.BYTES
        val filename = page.getString(filePosition)
        val blockPosition = filePosition + Page.maxLength(filename.length)
        val blockNumber = page.getInt(blockPosition)
        blockId = BlockId(filename, blockNumber)
        val offsetPosition = blockPosition + Integer.BYTES
        offset = page.getInt(offsetPosition)
        val valuePosition = offsetPosition + Integer.BYTES
        value = page.getString(valuePosition)
    }

    override fun op(): Int {
        return Operator.SETSTRING.id
    }

    override fun txNumber(): Int {
        return transactionNumber
    }

    override fun toString(): String {
        return "<SETSTRING $transactionNumber $blockId $offset $value>"
    }

    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        transaction.setString(blockId, offset, value, false) // don't log the undo
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(logManager: LogManager, txNum: Int, blk: BlockId, offset: Int, value: String): Int {
            val transactionPosition = Integer.BYTES
            val filePosition = transactionPosition + Integer.BYTES
            val blockPosition = filePosition + Page.maxLength(blk.filename.length)
            val offsetPosition = blockPosition + Integer.BYTES
            val valuePosition = offsetPosition + Integer.BYTES
            val recordLength = valuePosition + Page.maxLength(value.length)
            val record = ByteArray(recordLength)
            val p = Page(record)
            p.setInt(0, Operator.SETSTRING.id)
            p.setInt(transactionPosition, txNum)
            p.setString(filePosition, blk.filename)
            p.setInt(blockPosition, blk.number)
            p.setInt(offsetPosition, offset)
            p.setString(valuePosition, value)
            return logManager.append(record)
        }
    }
}