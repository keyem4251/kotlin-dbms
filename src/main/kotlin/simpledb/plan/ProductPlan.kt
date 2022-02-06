package simpledb.plan

import simpledb.query.ProductScan
import simpledb.query.Scan
import simpledb.record.Schema

class ProductPlan(
    private val plan1: Plan,
    private val plan2: Plan,
) : Plan {
    private val schema = Schema()

    init {
        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    override fun open(): Scan {
        val scan1 = plan1.open()
        val scan2 = plan2.open()
        return ProductScan(scan1, scan2)
    }

    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + (plan1.recordsOutput() * plan2.recordsOutput())
    }

    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * plan2.recordsOutput()
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