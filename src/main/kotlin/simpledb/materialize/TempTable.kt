package simpledb.materialize

import simpledb.query.UpdateScan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

class TempTable(
    private val transaction: Transaction,
    private val schema: Schema,
) {
    val tableName = nextTableName()
    val layout = Layout(schema)

    fun open(): UpdateScan {
        return TableScan(transaction, tableName, layout)
    }

    companion object {
        var nextTableNum: Int = 0

        @Synchronized
        fun nextTableName(): String {
            nextTableNum += 1
            return "temp$nextTableNum"
        }
    }
}