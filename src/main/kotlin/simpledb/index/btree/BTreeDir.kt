package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.tx.Transaction

/**
 * B Treeへソートされた順序で値を挿入、分割を行う
 * B TreeのLeafのもとになるクラス
 * インデックスの行を持つ、Leafへの参照を持つ
 */
class BTreeDir(
    private val transaction: Transaction,
    private val layout: Layout,
    private val blockId: BlockId,
) {
    private var contents = BTPage(transaction, blockId, layout)
    private val filename = blockId.filename

    fun close() {
        contents.close()
    }

    fun search(searchKey: Constant): Int {
        var childBlockId = findChildBlock(searchKey)
        while (contents.getFlag() > 0) {
            contents.close()
            contents = BTPage(transaction, childBlockId, layout)
            childBlockId = findChildBlock(searchKey)
        }
        return childBlockId.number
    }

    fun makeNewRoot(entry: DirEntry) {
        val firstVal = contents.getDataVal(0)
        val level = contents.getFlag()
        val newBlockId = contents.split(0, level) // ie, transfer all the recs
        val oldRoot = DirEntry(firstVal, newBlockId.number)
        insertEntry(oldRoot)
        insertEntry(entry)
        contents.setFlag(level+1)
    }

    fun insert(entry: DirEntry): DirEntry? {
        if (contents.getFlag() == 0) return insertEntry(entry)
        val childBlockId = findChildBlock(entry.dataVal)
        val child = BTreeDir(transaction, layout, childBlockId)
        val myEntry = child.insert(entry)
        child.close()
        return if (myEntry != null) {
            insertEntry(myEntry)
        } else {
            null
        }
    }

    private fun insertEntry(entry: DirEntry): DirEntry? {
        val newSlot = 1 + contents.findSlotBefore(entry.dataVal)
        contents.insertDir(newSlot, entry.dataVal, entry.blockNumber)
        if (!contents.isFull()) return null
        // else page is full, so split it
        val level = contents.getFlag()
        val splitPosition = contents.getNumRecords() / 2
        val splitVal = contents.getDataVal(splitPosition)
        val newBlockId = contents.split(splitPosition, level)
        return DirEntry(splitVal, newBlockId.number)
    }

    private fun findChildBlock(searchKey: Constant): BlockId {
        var slot = contents.findSlotBefore(searchKey)
        if (contents.getDataVal(slot+1).equals(searchKey)) slot += 1
        val blockNumber = contents.getChildNum(slot)
        return BlockId(filename, blockNumber)
    }
}
