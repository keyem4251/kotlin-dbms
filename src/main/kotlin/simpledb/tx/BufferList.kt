package simpledb.tx

import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.file.BlockId

/**
 * トランザクションで管理されてるバッファのリストクラス
 * どのブロックにバッファが結びつけられてるか、どのブロックが何回関連付けられてるかを記録
 */
class BufferList(private val bufferManager: BufferManager) {
    private val buffers = mutableMapOf<BlockId, Buffer>()
    private val pins = mutableListOf<BlockId>()

    fun getBuffer(blockId: BlockId): Buffer? {
        return buffers[blockId]
    }

    fun pin(blockId: BlockId) {
        val buffer = bufferManager.pin(blockId)
        buffers[blockId] = buffer
        pins.add(blockId)
    }

    fun unpin(blockId: BlockId) {
        val buffer = buffers[blockId]
        if (buffer != null) {
            bufferManager.unpin(buffer)
            pins.remove(blockId)
            if (!pins.contains(blockId)) buffers.remove(blockId)
        }
    }

    /**
     * トランザクションに結び付けられてるバッファを管理からすべて外す
     */
    fun unpinAll() {
        for (blockId in pins) {
            val buffer = buffers[blockId]
            if (buffer != null) bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }
}
