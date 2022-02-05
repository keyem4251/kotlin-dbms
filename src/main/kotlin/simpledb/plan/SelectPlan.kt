package simpledb.plan

import simpledb.query.Predicate
import simpledb.query.Scan
import simpledb.query.SelectScan
import simpledb.record.Schema

class SelectPlan(
    private val plan: Plan,
    private val predicate: Predicate,
) : Plan {

    override fun open(): Scan {
        val scan = plan.open()
        return SelectScan(scan, predicate)
    }

    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return plan.recordsOutput() / predicate.reductionFactor(plan)
    }

    override fun distinctValues(fieldName: String): Int {
        return if (predicate.equateWithConstant(fieldName) != null) {
            1
        } else {
            val fieldName2 = predicate.equatesWithField(fieldName)
            if (fieldName2 != null) {
                plan.distinctValues(fieldName).coerceAtMost(plan.distinctValues(fieldName2))
            } else {
                plan.distinctValues(fieldName)
            }
        }
    }

    override fun schema(): Schema {
        return plan.schema()
    }
}