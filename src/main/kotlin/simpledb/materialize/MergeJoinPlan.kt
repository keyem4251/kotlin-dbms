package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.tx.Transaction

class MergeJoinPlan(
    private val transaction: Transaction,
    private var plan1: Plan,
    private var plan2: Plan,
    private val fieldName1: String,
    private val fieldName2: String,
) : Plan {
    private val schema = Schema()
    init {
        val sortList1 = mutableListOf<String>(fieldName1)
        plan1 = SortPlan(plan1, transaction, sortList1)

        val sortList2 = mutableListOf<String>(fieldName2)
        plan2 = SortPlan(plan2, transaction, sortList2)

        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    override fun open(): Scan {
        val scan: Scan = plan1.open()
        val sortScan: SortScan = plan2.open() as SortScan
        return MergeJoinScan(scan, sortScan, fieldName1, fieldName2)
    }

    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + plan2.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        val maxValues = plan1.distinctValues(fieldName1).coerceAtLeast(plan2.distinctValues(fieldName2))
        return (plan1.recordsOutput() * plan2.recordsOutput()) / maxValues
    }

    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    override fun schema(): Schema {
        return schema
    }
}