package simpledb.tx.concurrency

import simpledb.file.BlockId

/**
 * ConcurrencyManagerはACID特性の一貫性と独立性を持つ
 * ロックの仕組みを使って実現する
 * 各トランザクションごとに作成される（LockTableは複数のトランザクションが同じものを参照する）
 * @property locks 各ブロックが共有ロックか、排他ロックかを保持する
 */
class ConcurrencyManager {
    private var locks = mutableMapOf<BlockId, String>()

    companion object {
        private var lockTable = LockTable()
    }

    /**
     * トランザクションがブロックにすでにロックを獲得しているかチェックし、共有ロックを獲得する
     */
    fun sLock(blockId: BlockId) {
        if (locks[blockId] == null) {
            // すでにロックが獲得されていなければロックを獲得
            lockTable.sLock(blockId)
            locks[blockId] = "S"
        }
    }

    /**
     * トランザクションがブロックにすでに排他ロックを獲得しているかチェックし、ロックを行う
     */
    fun xLock(blockId: BlockId) {
        if (!hasXLock(blockId)) {
            // 排他ロックを獲得していなければ
            // xLockをする場合は暗黙的にsLockも行う
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