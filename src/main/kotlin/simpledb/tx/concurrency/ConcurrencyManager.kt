package simpledb.tx.concurrency

import simpledb.file.BlockId

class ConcurrencyManager {
    private var lockTable = LockTable()
    private var locks = mutableMapOf<BlockId, String>()

    fun sLock(blockId: BlockId) {
        if (locks[blockId] == null) {
            lockTable.sLock(blockId)
            locks[blockId] = "S"
        }
    }

    fun xLock(blockId: BlockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId)
            lockTable.xLock(blockId)
            locks[blockId] = "X"
        }
    }

    fun release() {
        for (blockId in locks.keys) {
            lockTable.unlock(blockId)
        }
        locks.clear()
    }

    private fun hasXLock(blockId: BlockId): Boolean {
        val lockType = locks[blockId]
        return lockType != null && lockType.equals("X")
    }
}