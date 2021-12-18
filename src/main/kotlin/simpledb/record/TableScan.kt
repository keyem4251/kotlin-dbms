package simpledb.record

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.query.UpdateScan
import simpledb.tx.Transaction

class TableScan(
    private val transaction: Transaction,
    private val tableName: String,
    private val layout: Layout,
) : UpdateScan {
    private lateinit var recordPage: RecordPage
    private var fileName: String = ""
    private var currentSlot: Int = 0

    init {
        fileName = "$tableName.tbl"
        if (transaction.size(fileName) == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    // methods that implement Scan
    override fun close() {
        if (recordPage != null) {
            transaction.unpin(recordPage.blockId)
        }
    }

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
        currentSlot = recordPage.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) return false
            moveToBlock(recordPage.blockId.number+1)
            currentSlot = recordPage.nextAfter(currentSlot)
        }
        return true
    }

    override fun getInt(fieldName: String): Int {
        return recordPage.getInt(currentSlot, fieldName)
    }

    override fun getString(fieldName: String): String {
        return recordPage.getString(currentSlot, fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            return Constant(getInt(fieldName))
        } else {
            return Constant(getString(fieldName))
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    private fun moveToBlock(blockNumber: Int) {
        close()
        val blockId = BlockId(fileName, blockNumber)
        recordPage = RecordPage(transaction, blockId, layout)
        currentSlot = -1
    }

    private fun moveToNewBlock() {
        close()
        val blockId = transaction.append(fileName)
        recordPage = RecordPage(transaction, blockId, layout)
        recordPage.format()
        currentSlot = -1
    }

    private fun atLastBlock(): Boolean {
        return recordPage.blockId.number == (transaction.size(fileName) - 1)
    }
}