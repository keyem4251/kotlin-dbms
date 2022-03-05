package simpledb.multibuffer

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.Layout
import simpledb.record.RecordPage
import simpledb.tx.Transaction

class ChunkScan(
    private val transaction: Transaction,
    private val fileName: String,
    private val layout: Layout,
    private val startBufferNumber: Int,
    private val endBufferNumber: Int,
) : Scan {
    private val buffers = mutableListOf<RecordPage>()
    private var currentBlockNumber = 0
    private lateinit var recordPage: RecordPage
    private var currentSlot = 0

    init {
        for (i in startBufferNumber..endBufferNumber) {
            val blockId = BlockId(fileName, i)
            buffers.add(RecordPage(transaction, blockId, layout))
        }
        moveToBlock(startBufferNumber)
    }

    override fun close() {
        for (i in 0..buffers.size) {
            val blockId = BlockId(fileName, startBufferNumber+i)
            transaction.unpin(blockId)
        }
    }

    override fun beforeFirst() {
        moveToBlock(startBufferNumber)
    }

    override fun next(): Boolean {
        currentSlot = recordPage.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (currentBlockNumber == endBufferNumber) return false
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
        return if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    private fun moveToBlock(blockNumber: Int) {
        currentBlockNumber = blockNumber
        recordPage = buffers[currentBlockNumber - startBufferNumber]
        currentSlot = -1
    }
}