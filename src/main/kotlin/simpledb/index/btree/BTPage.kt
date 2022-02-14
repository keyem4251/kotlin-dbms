package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.tx.Transaction

class BTPage(
    private val transaction: Transaction,
    private var currentBlock: BlockId?,
    private val layout: Layout,
) {
    init {
        transaction.pin(currentBlock ?: throw RuntimeException("null error"))
    }

    fun findSlotBefore(searchKey: Constant): Int {
        var slot = 0
        while (slot < getNumRecords() && getDataVal(slot).compareTo(searchKey) < 0) {
            slot += 1
        }
        return slot - 1
    }

    fun close() {
        if (currentBlock != null) transaction.unpin(currentBlock!!)
        currentBlock = null
    }

    fun isFull(): Boolean {
        return slotPosition(getNumRecords()+1) >= transaction.blockSize()
    }

    fun split(splitPosition: Int, flag: Int): BlockId {
        val newBlock = appendNew(flag)
        val newPage = BTPage(transaction, newBlock, layout)
        transferRecords(splitPosition, newPage)
        newPage.setFlag(flag)
        newPage.close()
        return newBlock
    }

    fun getDataVal(slot: Int): Constant {
        return getVal(slot, "dataval")
    }

    fun gteFlag(): Int {
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        return transaction.getInt(nonNullBlock, 0) ?: throw RuntimeException("null error")
    }

    fun setFlag(value: Int) {
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        transaction.setInt(nonNullBlock, 0, value, true)
    }

    fun appendNew(flag: Int): BlockId {
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        val blockId = transaction.append(nonNullBlock.filename)
        transaction.pin(blockId)
        format(blockId, flag)
        return blockId
    }

    fun format(blockId: BlockId, flag: Int) {
        transaction.setInt(blockId, 0, flag, false)
        transaction.setInt(blockId, Integer.BYTES, 0, false) // records = 0
        val recordSize = layout.slotSize()
        var position = 2 * Integer.BYTES
        while ((position+recordSize) <= transaction.blockSize()) {
            makeDefaultRecord(blockId, position)
            position += recordSize
        }
    }

    private fun makeDefaultRecord(blockId: BlockId, position: Int) {
        for (fieldName in layout.schema().fields) {
            val offset = layout.offset(fieldName) ?: throw RuntimeException("null error")
            if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
                transaction.setInt(blockId, position + offset, 0, false)
            } else {
                transaction.setString(blockId, position + offset, "", false)
            }
        }
    }

    // Methods called only by BTreeDir
    fun getChildNum(slot: Int): Int {
        return getInt(slot, "block")
    }

    fun insertDir(slot: Int, value: Constant, blockNumber: Int) {
        insert(slot)
        setVal(slot, "dataval", value)
        setInt(slot, "block", blockNumber)
    }

    // Methods called only by BTreeLeaf
    fun getDataRid(slot: Int): RID {
        return RID(getInt(slot, "block"), getInt(slot, "id"))
    }

    fun insertLeaf(slot: Int, value: Constant, rid: RID) {
        insert(slot)
        setVal(slot, "dataval", value)
        setInt(slot, "block", rid.blockNumber)
        setInt(slot, "id", rid.slot)
    }

    fun delete(slot: Int) {
        for (i in slot+1 until getNumRecords()) {
            copyRecord(i, i-1)
        }
        setNumRecords(getNumRecords()-1)
        return
    }

    fun getNumRecords(): Int {
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        return transaction.getInt(nonNullBlock, Integer.BYTES) ?: throw RuntimeException("null error")
    }

    // Private methods
    private fun getInt(slot: Int, fieldName: String): Int {
        val position = fieldPosition(slot, fieldName)
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        return transaction.getInt(nonNullBlock, position) ?: throw RuntimeException("null error")
    }

    private fun getString(slot: Int, fieldName: String): String {
        val position = fieldPosition(slot, fieldName)
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        return transaction.getString(nonNullBlock, position) ?: throw RuntimeException("null error")
    }

    private fun getVal(slot: Int, fieldName: String): Constant {
        val type = layout.schema().type(fieldName)
        return if (type == java.sql.Types.INTEGER) {
            Constant(getInt(slot, fieldName))
        } else {
            Constant(getString(slot, fieldName))
        }
    }

    private fun setInt(slot: Int, fieldName: String, value: Int) {
        val position = fieldPosition(slot, fieldName)
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        transaction.setInt(nonNullBlock, position, value, true)
    }

    private fun setString(slot: Int, fieldName: String, value: String) {
        val position = fieldPosition(slot, fieldName)
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        transaction.setString(nonNullBlock, position, value, true)
    }

    private fun setVal(slot: Int, fieldName: String, value: Constant) {
        val type = layout.schema().type(fieldName)
        if (type == java.sql.Types.INTEGER) {
            setInt(slot, fieldName, value.asInt() ?: throw RuntimeException("null error"))
        } else {
            setString(slot, fieldName, value.asString() ?: throw RuntimeException("null error"))
        }
    }

    private fun setNumRecords(n: Int) {
        val nonNullBlock = currentBlock ?: throw RuntimeException("null error")
        transaction.setInt(nonNullBlock, Integer.BYTES, n, true)
    }

    private fun insert(slot: Int) {
        for (i in getNumRecords() downTo slot) {
            copyRecord(i-1, i)
        }
        setNumRecords(getNumRecords()+1)
    }

    private fun copyRecord(from: Int, to: Int) {
        val schema = layout.schema()
        for (fieldName in schema.fields) {
            setVal(to, fieldName, getVal(from, fieldName))
        }
    }

    private fun fieldPosition(slot: Int, fieldName: String): Int {
        val offset = layout.offset(fieldName) ?: throw RuntimeException("null error")
        return slotPosition(slot) + offset
    }

    private fun transferRecords(slot: Int, dest: BTPage) {
        var destSlot = 0
        while (slot < getNumRecords()) {
            dest.insert(destSlot)
            val schema = layout.schema()
            for (fieldName in schema.fields) {
                dest.setVal(destSlot, fieldName, getVal(slot, fieldName))
            }
            delete(slot)
            destSlot += 1
        }
    }

    private fun slotPosition(slot: Int): Int {
        val slotSize = layout.slotSize()
        return Integer.BYTES + Integer.BYTES + (slot * slotSize)
    }
}