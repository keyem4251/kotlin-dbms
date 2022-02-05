package simpledb.tx.concurrency

import simpledb.file.BlockId

/**
 * ブロックへのロックを管理するクラス
 * @property locks それぞれのブロックにロックを持っているかをブロックIDと数値で管理する
 * トランザクションがブロックを排他ロックしている場合は-1、正の数値の場合はその数分トランザクションが共有ロックを獲得している
 */
class LockTable {
    private val maxTime: Long = 10000
    private val locks = mutableMapOf<BlockId, Int>()
    private val lock = Object()

    /**
     * 共有ロック(shared lock)
     * トランザクションがブロックを保持してる場合、他のトランザクションは共有ロックだけ獲得できる
     * 他のトランザクションが排他ロックを獲得してる場合はwaitする
     */
    fun sLock(blockId: BlockId) {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                // ループの条件が成立してる間クライアントのスレッドは待ち行列に入っている
                while (hasXLock(blockId) && !waitingTooLong(timestamp)) lock.wait(maxTime)
                if (hasXLock(blockId)) throw LockAbortException()

                val lockValue = getLockValue(blockId) // will not be negative
                locks[blockId] = lockValue + 1
            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    /**
     * 排他ロック(exclusive lock)
     * トランザクションがブロックを保持してる場合、他のトランザクションはロックを獲得することができない
     * 他のトランザクションが共有ロックを獲得してる場合はwaitする
     */
    fun xLock(blockId: BlockId) {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                // ループの条件が成立してる間クライアントのスレッドは待ち行列に入っている
                while (hasOthersLocks(blockId) && !waitingTooLong(timestamp)) lock.wait(maxTime)
                if (hasOthersLocks(blockId)) throw LockAbortException()

                locks[blockId] = -1
            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    fun unlock(blockId: BlockId) {
        synchronized(lock) {
            val lockValue = getLockValue(blockId)
            if (lockValue > 1) {
                // 共有ロックの場合
                locks[blockId] = lockValue - 1
            } else {
                // 排他ロックの場合 スレッドの待ちを開放し他のスレッドがロックを獲得できるようになる
                // Javaのスレッドスケジューラーが待ちスレッドを開始する
                locks.remove(blockId)
                lock.notifyAll()
            }
        }
    }

    private fun hasXLock(blockId: BlockId): Boolean {
        return getLockValue(blockId) < 0
    }

    private fun hasOthersLocks(blockId: BlockId): Boolean {
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