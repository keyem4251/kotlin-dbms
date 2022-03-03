package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.tx.Transaction

class GroupByPlan(
    private val transaction: Transaction,
    private var plan: Plan,
    private val groupFields: List<String>,
    private val aggregationFns: List<AggregationFn>,
) : Plan {
    private val schema = Schema()

    init {
        plan = SortPlan(plan, transaction, groupFields)
        for (fieldName in groupFields) {
            schema.add(fieldName, plan.schema())
        }
        for (aggregationFn in aggregationFns) {
            schema.addIntField(aggregationFn.fieldName())
        }
    }

    override fun open(): Scan {
        val scan: Scan = plan.open()
        return GroupByScan(scan, groupFields, aggregationFns)
    }

    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        var numberGroups = 1
        for (fieldName in groupFields) {
            numberGroups *= plan.distinctValues(fieldName)
        }
        return numberGroups
    }

    override fun distinctValues(fieldName: String): Int {
        return if (plan.schema().hasField(fieldName)) {
            plan.distinctValues(fieldName)
        } else {
            recordsOutput()
        }
    }

    override fun schema(): Schema {
        return schema
    }
}