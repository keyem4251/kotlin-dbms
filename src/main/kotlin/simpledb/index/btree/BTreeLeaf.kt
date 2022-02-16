package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.tx.Transaction

class BTreeLeaf(
    private val transaction: Transaction,
    private val layout: Layout,
    private val searchKey: Constant,
    private val blockId: BlockId,
) {
    private var contents: BTPage = BTPage(transaction, blockId, layout)
    private var currentSlot = contents.findSlotBefore(searchKey)
    private val filename = blockId.filename

    fun close() {
        contents.close()
    }

    fun next(): Boolean {
        currentSlot += 1
        return if (currentSlot >= contents.getNumRecords()) {
            tryOverflow()
        } else if (contents.getDataVal(currentSlot).equals(searchKey)) {
            true
        } else {
            tryOverflow()
        }
    }

    fun getDataRid(): RID {
        return contents.getDataRid(currentSlot)
    }

    fun delete(dataRid: RID) {
        while (next()) {
            if (getDataRid().equals(dataRid)) {
                contents.delete()
                return
            }
        }
    }

    fun insert(dataRid: RID): DirEntry? {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            val firstVal = contents.getDataVal(0)
            val newBlockInt = contents.split(0, contents.getFlag())
            currentSlot = 0
            contents.setFlag(-1)
            contents.insertLeaf(currentSlot, searchKey, dataRid)
            return DirEntry(firstVal, newBlockInt.number)
        }
        currentSlot += 1
        contents.insertLeaf(currentSlot, searchKey, dataRid)
        if (!contents.isFull()) return null
        // else page is full, so split it
        val firstKey = contents.getDataVal(0)
        val lastKey = contents.getDataVal(contents.getNumRecords()-1)
        if (lastKey.equals(firstKey)) {
            // create an overflow block to hold all but the first record
            val newBlockId = contents.split(1, contents.getFlag())
            contents.setFlag(newBlockId.number)
            return null
        } else {
            var splitPosition = contents.getNumRecords() / 2
            var splitKey = contents.getDataVal(splitPosition)
            if (splitKey.equals(firstKey)) {
                // move right, looking for the next key
                while (contents.getDataVal(splitPosition).equals(splitPosition)) splitPosition += 1
                splitKey = contents.getDataVal(splitPosition)
            } else {
                // move left, looking for first entry having that key
                while (contents.getDataVal(splitPosition-1).equals(splitKey)) splitPosition -= 1
            }
            val newBlockId = contents.split(splitPosition, -1)
            return DirEntry(splitKey, newBlockId.number)
        }
    }

    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataVal(0)
        val flag = contents.getFlag()
        if (searchKey != firstKey || flag < 0) return false
        contents.close()
        val nextBlockId = BlockId(filename, flag)
        contents = BTPage(transaction, nextBlockId, layout)
        currentSlot = 0
        return true
    }
}