package simpledb.record

import simpledb.file.BlockId
import simpledb.tx.Transaction

enum class RecordPageState(val id: Int) {
    EMPTY(0),
    USED(1),
}

class RecordPage(
    private val transaction: Transaction,
    private val blockId: BlockId,
    private val layout: Layout,
) {
    init {
        transaction.pin(blockId)
    }

    fun getInt(slot: Int, folderName: String): Int {
        val layoutOffset = layout.offset(folderName) ?: throw RecordPageException()
        val folderPosition = offset(slot) + layoutOffset
        return transaction.getInt(blockId, folderPosition) ?: throw RecordPageException()
    }

    fun getString(slot: Int, folderName: String): String {
        val layoutOffset = layout.offset(folderName) ?: throw RecordPageException()
        val folderPosition = offset(slot) + layoutOffset
        return transaction.getString(blockId, folderPosition) ?: throw RecordPageException()
    }

    fun setInt(slot: Int, folderName: String, value: Int) {
        val layoutOffset = layout.offset(folderName) ?: throw RecordPageException()
        val folderPosition = offset(slot) + layoutOffset
        transaction.setInt(blockId, folderPosition, value, true)
    }

    fun setString(slot: Int, folderName: String, value: String) {
        val layoutOffset = layout.offset(folderName) ?: throw RecordPageException()
        val folderPosition = offset(slot) + layoutOffset
        transaction.setString(blockId, folderPosition, value, true)
    }

    fun delete(slot: Int) {
        setFlag(slot, RecordPageState.EMPTY.id)
    }

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), RecordPageState.EMPTY.id, false)
            val schema = layout.schema()
            for (folderName in schema.fields) {
                val layoutOffset = layout.offset(folderName) ?: throw RecordPageException()
                val folderPosition = offset(slot) + layoutOffset
                if (schema.type(folderName) == java.sql.Types.INTEGER) {
                    transaction.setInt(blockId, folderPosition, 0, false)
                } else {
                    transaction.setString(blockId, folderPosition, "", false)
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, RecordPageState.USED.id)
    }

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, RecordPageState.EMPTY.id)
        if (newSlot >= 0) setFlag(newSlot, RecordPageState.USED.id)
        return newSlot
    }

    fun blockId(): BlockId {
        return blockId
    }

    private fun searchAfter(slot: Int, flag: Int): Int {
        var nextSlot = slot + 1
        while (isValidSlot(nextSlot)) {
            val transactionInt = transaction.getInt(blockId, offset(nextSlot))
            if (transactionInt != null && transactionInt == flag) {
                return nextSlot
            }
            nextSlot++
        }
        return -1
    }

    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot+1) <= transaction.blockSize()
    }

    // Private auxiliary methods
    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(blockId, offset(slot), flag, true)
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}