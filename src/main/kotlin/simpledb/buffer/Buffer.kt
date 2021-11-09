package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page
import simpledb.log.LogManager

class Buffer(
    val fm: FileManager,
    val lm: LogManager,
) {
    private var contents: Page = Page(fm.blockSize)
    private lateinit var blockId: BlockId
    private var pins = 0
    private var txnum = -1
    private var lsn = -1

    fun contents(): Page {
        return contents
    }

    fun blockId(): BlockId {
        return blockId
    }

    fun setModified(newTxnum: Int, newLsn: Int) {
        txnum = newTxnum
        if (lsn >= 0) lsn = newLsn
    }

    fun isPinned(): Boolean {
        return pins > 0
    }

    fun modifyingTx(): Int {
        return txnum
    }

    fun assignToBlock(b: BlockId) {
        flush()
        blockId = b
        fm.read(blockId, contents)
        pins = 0
    }

    fun flush() {
        if (txnum >= 0) {
            lm.flush(lsn)
            fm.write(blockId, contents)
            txnum = -1
        }
    }

    fun pin() {
        pins++
    }

    fun unpin() {
        pins--
    }
}