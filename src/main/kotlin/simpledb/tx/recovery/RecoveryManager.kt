package simpledb.tx.recovery

import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.log.LogManager
import simpledb.tx.Transaction

/**
 * RecoverManagerはACID特性の原子性と耐久性を持つ
 * コミットとロールバックの操作を使って実現する
 * 各トランザクションはそれぞれRecoverManagerを持つ
 */
class RecoveryManager(
    private val transaction: Transaction,
    private val transactionNumber: Int,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {
    /**
     * 特定のトランザクションに紐づくRecoverManagerを作成する
     * トランザクションの処理が開始されるのでStartRecordを記録する
     */
    init {
        StartRecord.writeToLog(logManager, transactionNumber)
    }

    /**
     * 関連付けられたトランザクションの識別子を元にトランザクションの内容をディスクに書き出す
     * コミットの行をログに作成し、ログに書き出す
     */
    fun commit() {
        bufferManager.flushAll(transactionNumber)
        val lsn = CommitRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    /**
     * doRollbackを実行し
     * 関連付けられたトランザクションの識別子を元にトランザクションの内容をディスクに書き出す
     * ロールバックの行をログに作成し、ログに書き出す
     */
    fun rollback() {
        doRollback()
        bufferManager.flushAll(transactionNumber)
        val lsn = RollbackRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    /**
     * doRecoverを実行し
     * ログから完了していないトランザクションの内容を回復させる
     * チェックポイントの行をログに作成し、ログに書き出す
     */
    fun recover() {
        doRecover()
        bufferManager.flushAll(transactionNumber)
        val lsn = CheckpointRecord.writeToLog(logManager)
        logManager.flush(lsn)
    }

    /**
     * 数値を設定する行を作成し、ログに書き出し、ログの識別子を返す
     * ページを含む[buffer]を受け取り、ページの値の位置[offset]から元の値を取り出しログを作成
     */
    fun setInt(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getInt(offset)
        val blockId = buffer.blockId() ?: throw RuntimeException("null error")
        // TODO: setIntRecordにセットする値は新しい値？
        return SetIntRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
    }

    /**
     * 文字列を設定する行を作成し、ログに書き出し、ログの識別子を返す
     * ページを含む[buffer]を受け取り、ページの値の位置[offset]から元の値を取り出しログを作成
     */
    fun setString(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getString(offset)
        val blockId = buffer.blockId() ?: throw RuntimeException("null error")
        // TODO: setStringRecordにセットする値は新しい値？
        return SetStringRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue)
    }

    /**
     * トランザクションをロールバックする
     * ログのイテレータからトランザクションの開始の行（Operator.START）まで処理を戻す
     */
    private fun doRollback() {
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes)
            if (record != null && record.txNumber() == transactionNumber) {
                if (record.op() == Operator.START.id) return
                record.undo(transaction)
            }
        }
    }

    /**
     * データベースの復旧を行う
     * ログのイテレータから値を取り出し、未完了のトランザクションのログの行を見つける度にundo()を呼び出す
     * CHECKPOINTまたはログの終わりまで行くと停止する
     */
    private fun doRecover() {
        val finishedTransaction = mutableListOf<Int>()
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes) ?: return
            if (record.op() == Operator.CHECKPOINT.id) return

            if (record.op() == Operator.COMMIT.id || record.op() == Operator.ROLLBACK.id) {
                finishedTransaction.add(record.txNumber())
            } else if (!finishedTransaction.contains(record.txNumber())) {
                record.undo(transaction)
            }
        }
    }
}
