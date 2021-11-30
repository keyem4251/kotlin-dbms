package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

/**
 * @property blockId: 変更されたファイルの名前とブロック番号
 * @property offset: 変更が発生したオフセット
 * @property value: 変更が発生したオフセットでの古い値
 * 変更が発生したオフセットでの新しい値
 */
class SetIntRecord(val page: Page): LogRecord {
    private var transactionNumber: Int
    private var offset: Int
    private var value: Int
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
        value = page.getInt(valuePosition)
    }

    override fun op(): Int {
        return Operator.SETINT.id
    }

    override fun txNumber(): Int {
        return transactionNumber
    }

    override fun toString(): String {
        return "<SETINT $transactionNumber $blockId $offset $value>"
    }

    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        transaction.setInt(blockId, offset, value, false) // don't log the undo
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(logManager: LogManager, transactionNumber: Int, blockId: BlockId, offset: Int, value: Int): Int {
            val transactionPosition = Integer.BYTES
            val filePosition = transactionPosition + Integer.BYTES
            val blockPosition = filePosition + Page.maxLength(blockId.filename.length)
            val offsetPosition = blockPosition + Integer.BYTES
            val valuePosition = offsetPosition + Integer.BYTES
            val record = ByteArray(valuePosition + Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.SETINT.id)
            page.setInt(transactionPosition, transactionNumber)
            page.setString(filePosition, blockId.filename)
            page.setInt(blockPosition, blockId.number)
            page.setInt(offsetPosition, offset)
            page.setInt(valuePosition, value)
            return logManager.append(record)
        }
    }
}