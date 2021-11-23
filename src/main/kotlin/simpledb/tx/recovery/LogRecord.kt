package simpledb.tx.recovery

import simpledb.file.Page

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
                Operator.CHECKPOINT.id -> CheckPointRecord()
                Operator.START.id -> StartRecord()
                Operator.COMMIT.id -> CommitRecord()
                Operator.ROLLBACK.id -> RollbackRecord()
                Operator.SETINT.id -> SetIntRecord()
                Operator.SETSTRING.id -> SetStringRecord()
            }
            return null
        }
    }
}