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

interface LogRecord {
    fun op(): Int
    fun txNumber(): Int
    fun undo(transaction: Transaction)

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