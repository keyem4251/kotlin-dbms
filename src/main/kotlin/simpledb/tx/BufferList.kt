package simpledb.tx

import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.file.BlockId

class BufferList(val bufferManager: BufferManager) {
    private val buffers = mutableMapOf<BlockId, Buffer>()
    private val pins = mutableListOf<BlockId>()

    fun getBuffer(blockId: BlockId): Buffer {
        
    }
}