package simpledb.tx.concurrency

import simpledb.file.BlockId

class LockTable {
    private val maxTime: Long = 10000
    private val locks = mutableMapOf<BlockId, Int>()
    private val lock = java.lang.Object()

    fun sLock(blockId: BlockId) {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                while (hasXLock(blockId) && !waitingTooLong(timestamp)) lock.wait(maxTime)
                if (hasXLock(blockId)) throw LockAbortException()

                val lockValue = getLockValue(blockId) // will not be negative
                locks.put(blockId, lockValue + 1)
            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    fun xLock(blockId: BlockId) {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                while (hasOtherSLocks(blockId) && !waitingTooLong(timestamp)) lock.wait(maxTime)
                if (hasOtherSLocks(blockId)) throw LockAbortException()

                locks.put(blockId, -1)
            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    fun unlock(blockId: BlockId) {
        synchronized(lock) {
            val lockValue = getLockValue(blockId)
            if (lockValue > 1) {
                locks.put(blockId, lockValue - 1)
            } else {
                locks.remove(blockId)
                lock.notifyAll()
            }
        }
    }

    private fun hasXLock(blockId: BlockId): Boolean {
        return getLockValue(blockId) < 0
    }

    private fun hasOtherSLocks(blockId: BlockId): Boolean {
        return getLockValue(blockId) > 1
    }

    private fun waitingTooLong(startTime: Long): Boolean {
        return System.currentTimeMillis() - startTime > maxTime
    }

    private fun getLockValue(blockId: BlockId): Int {
        val intValue = locks[blockId]
        return intValue?: 0
    }
}