package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.tx.Transaction

enum class Operator(val id: Int) {
    CHECKPOINT(0),
    START(1),
    COMMIT(2),
    ROLLBACK(3),
    SETINT(4),
    SETSTRING(5),
}

/**
 * RecoveryManagerがTransactionの動きを記録するのに使用する。
 * 以下の3つの場合にさくせされる
 * Transactionが開始する際: start
 * Transactionが完了する際: commit, rollback
 * Transactionが値を修正する際: setint, setstring
 */
interface LogRecord {
    /**
     * レコードのタイプ（Operatorのid）
     */
    fun op(): Int

    /**
     * ログレコードに保存されているTransactionのID
     */
    fun txNumber(): Int

    /**
     * ログレコードでエンコードされた操作を元に戻す。
     * SETINT, SETSTRINGの場合に動作をする。
     */
    fun undo(transaction: Transaction)

    /**
     * Log IteratorによってreturnされたバイトからLogRecordを作成する
     */
    companion object {
        fun createLogRecord(byteArray: ByteArray): LogRecord? {
            val p = Page(byteArray)
            when (p.getInt(0)) {
                Operator.CHECKPOINT.id -> CheckpointRecord()
                Operator.START.id -> StartRecord(p)
                Operator.COMMIT.id -> CommitRecord(p)
                Operator.ROLLBACK.id -> RollbackRecord(p)
                Operator.SETINT.id -> SetIntRecord(p)
                Operator.SETSTRING.id -> SetStringRecord(p)
            }
            return null
        }
    }
}