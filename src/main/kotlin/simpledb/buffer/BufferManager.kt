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
 * @property MAX_TIME バッファの空きを探す時間
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
                // bufferが使用できる場合は使用できる数（numAvailable）を増やし、スレッドを開放
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
                    // 使用できるバッファがない場合スレッドを待機させる
                    // 他のスレッドがバッファの使用をやめ、unpinを実行しnotifyAllを呼んだ場合
                    // MAX_TIMEの時間まで待った場合
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

    /**
     * 指定されたディスク[blockId]にfindExistingBufferを使ってすでにバッファが割り当てられているか確認
     * 割り当てられてなければchooseUnpinnedBufferを呼び、使用されていないバッファを探す
     * 使用されてないバッファがあれば、バッファにすでに割り当てられているPageをディスクに書き込み、
     * 指定した[blockId]をバッファに割り当てる
     * 使用できるバッファがなければnullを返す
     * @return 使用できるバッファ、ない場合はnull
     */
    private fun tryToPin(blockId: BlockId): Buffer? {
        var buffer = findExistingBuffer(blockId)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
            if (buffer == null) return null
            buffer.assignToBlock(blockId)
        }
        // 指定された[blockId]が割り当てられているバッファが現在使用されてなければ
        // 使用できる数を減らす（isPinnedがfalseなので現在はnumAvailableに含まれているがpinするので、今後は使用できる数に含まれない）
        if (!buffer.isPinned()) numAvailable--
        buffer.pin()
        return buffer
    }

    /**
     * 指定されたディスク[blockId]がバッファにすでに割り当てられていればバッファを返す
     * @return 割り当てられているバッファ
     */
    private fun findExistingBuffer(blockId: BlockId): Buffer? {
        for (buffer in bufferPool) {
            val bId = buffer.blockId()
            if (bId == blockId) {
                return buffer
            }
        }
        return null
    }

    /**
     * 使用されていないバッファを返す
     * @return 使用されていないバッファ
     */
    private fun chooseUnpinnedBuffer(): Buffer? {
        for (buffer in bufferPool) {
            if (!buffer.isPinned()) return buffer
        }
        return null
    }
}