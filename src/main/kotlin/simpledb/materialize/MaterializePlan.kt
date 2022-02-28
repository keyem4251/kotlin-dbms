package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.query.UpdateScan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction
import kotlin.math.ceil

class MaterializePlan(
    private val srcPlan: Plan,
    private val transaction: Transaction,
) {
    fun open(): Scan {
        val schema = srcPlan.schema()
        val tempTable = TempTable(transaction, schema)
        val srcScan: Scan = srcPlan.open()
        val destScan: UpdateScan = tempTable.open()
        while (srcScan.next()) {
            destScan.insert()
            for (fieldName in schema.fields) {
                destScan.setVal(fieldName, srcScan.getVal(fieldName))
            }
        }
        srcScan.close()
        destScan.beforeFirst()
        return destScan
    }

    fun blockAccessed(): Int {
        // create a dummy Layout object to calculate slot size
        val dummyLayout = Layout(srcPlan.schema())
        val rpb = (transaction.blockSize() / dummyLayout.slotSize()) as Double
        return ceil(srcPlan.recordsOutput() / rpb) as Int
    }

    fun recordsOutput(): Int {
        return srcPlan.recordsOutput()
    }

    fun distinctValues(fieldName: String): Int {
        return srcPlan.distinctValues(fieldName)
    }

    fun schema(): Schema {
        return srcPlan.schema()
    }
}