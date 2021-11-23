package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogManager

class SetStringRecord(val page: Page): LogRecord {
    private var transactionNumber: Int
    private var offset: Int
    private var value: String
    private lateinit var blockId: BlockId

    init {
        val tpos = Integer.BYTES
        transactionNumber = page.getInt(tpos)
        val fpos = tpos + Integer.BYTES
        val filename = page.getString(fpos)
        val bpos = fpos + Page.maxLength(filename.length)
        val blockNumber = page.getInt(bpos)
        blockId = BlockId(filename, blockNumber)
        val opos = bpos + Integer.BYTES
        offset = page.getInt(opos)
        val vpos = opos + Integer.BYTES
        value = page.getString(vpos)
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
            val tpos = Integer.BYTES
            val fpos = tpos + Integer.BYTES
            val bpos = fpos + Page.maxLength(blk.filename.length)
            val opos = bpos + Integer.BYTES
            val vpos = opos + Integer.BYTES
            val recordLength = vpos + Page.maxLength(value.length)
            val record = ByteArray(recordLength)
            val p = Page(record)
            p.setInt(0, Operator.SETSTRING.id)
            p.setInt(tpos, txNum)
            p.setString(fpos, blk.filename)
            p.setInt(bpos, blk.number)
            p.setInt(opos, offset)
            p.setString(vpos, value)
            return logManager.append(record)
        }
    }
}