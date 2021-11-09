package simpledb.log

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page

/**
 * システムの最初に作成される
 *
 */
class LogManager(
    private val fm: FileManager,
    private val logFile: String,
) {
    var logPage: Page
    var currentBlock: BlockId
    private var latestLSN = 0
    private var lastSavedLSN = 0

    init {
        val b = ByteArray(fm.blockSize)
        logPage = Page(b)

        val logSize = fm.length(logFile)
        if (logSize == 0) {
            currentBlock = appendNewBlock()
        } else {
            currentBlock = BlockId(logFile, logSize - 1)
            fm.read(currentBlock, logPage)
        }
    }

    fun flush(lsn: Int) {
        if (lsn >= lastSavedLSN) {
            flush()
        }
    }

    fun iterator(): Iterator<ByteArray> {
        flush()
        return LogIterator(fm, currentBlock)
    }

    @Synchronized
    fun append(logRecord: ByteArray): Int {
        var boundary = logPage.getInt(0)
        val recordSize = logRecord.size
        val bytesNeeded = recordSize + Integer.BYTES
        if (boundary - bytesNeeded < Integer.BYTES) { // It doesn't fit
            flush() // so move to the next block
            currentBlock = appendNewBlock()
            boundary = logPage.getInt(0)
        }
        val recrdPosition = boundary - bytesNeeded
        logPage.setBytes(recrdPosition, logRecord)
        logPage.setInt(0, recrdPosition) // the new boundary
        latestLSN += 1
        return latestLSN
    }

    private fun appendNewBlock(): BlockId {
        val block = fm.append(logFile)
        logPage.setInt(0, fm.blockSize)
        fm.write(block, logPage)
        return block
    }

    private fun flush() {
        fm.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}