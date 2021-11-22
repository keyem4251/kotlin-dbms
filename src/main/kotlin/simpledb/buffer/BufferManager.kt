package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.log.LogManager

/**
 * システムの起動時1つ作成される
 *
 * @property fm FileManagerクラス
 * @property lm LogManagerクラス
 * @property numBuffers バッファプールのサイズ
 * @property bufferPool 管理しているバッファ
 * @property numAvailable 空いているBufferの数
 * @property MAX_TIME
 * @property lock
 */
class BufferManager(
    val fm: FileManager,
    val lm: LogManager,
    var numBuffers: Int,
) {
    private lateinit var bufferPool: Array<Buffer>
    private var numAvailable = numBuffers
    private val MAX_TIME: Long = 10000 // 10 seconds
    private val lock = java.lang.Object()

    init {
        for (i in 0..numBuffers) {
            bufferPool[i] = Buffer(fm, lm)
        }
    }

    /**
     * @return 空いているBufferの数を返す
     */
    @Synchronized
    fun available(): Int {
        return numAvailable
    }

    /**
     * Bufferの内容をディスクに書き出す
     */
    @Synchronized
    fun flushAll(txnum: Int) {
        for (buffer in bufferPool) {
            if (buffer.modifyingTx() == txnum) buffer.flush()
        }
    }

    /**
     * 指定したBufferからPageを開放する
     */
    fun unpin(buffer: Buffer) {
        synchronized(lock) {
            buffer.unpin()
            if (!buffer.isPinned()) {
                numAvailable++
                lock.notifyAll()
            }
        }
    }

    /**
     * 特定のブロックの内容を含んだページとBufferを結びつける（BufferがPageを持つ）
     * @return バッファオブジェクト
     */
    fun pin(blockId: BlockId): Buffer {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                val buffer = tryToPin(blockId)
                while (buffer == null && !waitingTooLong(timestamp)) {
                    lock.wait(MAX_TIME)
                    val buffer = tryToPin(blockId)
                }
                if (buffer == null) throw BufferAbortException()

                return buffer
            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    private fun waitingTooLong(starttime: Long): Boolean {
        return System.currentTimeMillis() - starttime > MAX_TIME
    }

    private fun tryToPin(blockId: BlockId): Buffer? {
        var buffer = findExistingBuffer(blockId)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
            if (buffer == null) return null
            buffer.assignToBlock(blockId)
        }
        if (!buffer.isPinned()) numAvailable--
        buffer.pin()
        return buffer
    }

    private fun findExistingBuffer(blockId: BlockId): Buffer? {
        for (buffer in bufferPool) {
            val bId = buffer.blockId()
            if (bId == blockId) {
                return buffer
            }
        }
        return null
    }

    private fun chooseUnpinnedBuffer(): Buffer? {
        for (buffer in bufferPool) {
            if (!buffer.isPinned()) return buffer
        }
        return null
    }
}