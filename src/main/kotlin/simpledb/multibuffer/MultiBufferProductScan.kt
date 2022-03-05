package simpledb.multibuffer

import simpledb.query.Constant
import simpledb.query.ProductScan
import simpledb.query.Scan
import simpledb.record.Layout
import simpledb.tx.Transaction

class MultiBufferProductScan(
    private val transaction: Transaction,
    private val leftHandSideScan: Scan,
    private val fileName: String,
    private val layout: Layout,
) : Scan {
    private var rightHandSideScan: Scan? = null
    private lateinit var productScan: Scan
    private val fileSize = transaction.size(fileName)
    private val chunkSize = BufferNeeds.bestFactor(transaction.availableBuffers(), fileSize)
    private var nextBlockNumber = 0

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        nextBlockNumber = 0
        useNextChunk()
    }

    override fun next(): Boolean {
        while (!productScan.next()) {
            if (!useNextChunk()) return false
        }
        return true
    }

    override fun close() {
        productScan.close()
    }

    override fun getVal(fieldName: String): Constant {
        return productScan.getVal(fieldName)
    }

    override fun getInt(fieldName: String): Int {
        return productScan.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return productScan.getString(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return productScan.hasField(fieldName)
    }

    private fun useNextChunk(): Boolean {
        if (rightHandSideScan != null) rightHandSideScan!!.close()
        if (nextBlockNumber >= fileSize) return false
        var end = nextBlockNumber + chunkSize - 1
        if (end >= fileSize) end = fileSize - 1
        rightHandSideScan = ChunkScan(transaction, fileName, layout, nextBlockNumber, end)
        leftHandSideScan.beforeFirst()
        productScan = ProductScan(leftHandSideScan, rightHandSideScan!!)
        nextBlockNumber = end + 1
        return true
    }
}