package simpledb.log

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page

class LogIterator(val fm: FileManager, var blockId: BlockId): Iterator<ByteArray> {
    private var page: Page
    private var currentPosition = 0
    private var boundary = 0

    init {
        val b = ByteArray(fm.blockSize)
        page = Page(b)
        moveToBlock(blockId)
    }

    override fun hasNext(): Boolean {
        return currentPosition < fm.blockSize || blockId.number > 0
    }

    override fun next(): ByteArray {
        if (currentPosition == fm.blockSize) {
            blockId = BlockId(blockId.filename, blockId.number-1)
            moveToBlock(blockId)
        }
        val record = page.getBytes(currentPosition)
        currentPosition += Integer.BYTES + record.size
        return record
    }

    private fun moveToBlock(blk: BlockId) {
        fm.read(blk, page)
        boundary = page.getInt(0)
        currentPosition = boundary
    }
}