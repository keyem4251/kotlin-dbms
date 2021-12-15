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

    // Private auxiliary methods
    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(blockId, offset(slot), flag, true)
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}