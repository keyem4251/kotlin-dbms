package simpledb.plan

import simpledb.query.Scan
import simpledb.record.Schema

class OptimizedProductPlan(
    private val plan1: Plan,
    private val plan2: Plan,
) : Plan {
    private lateinit var bestPlan: Plan
    init {
        val productPlan1 = ProductPlan(plan1, plan2)
        val productPlan2 = ProductPlan(plan2, plan1)
        val blockAccessed1 = productPlan1.blocksAccessed()
        val blockAccessed2 = productPlan2.blocksAccessed()
        bestPlan = if (blockAccessed1 < blockAccessed2) {
            productPlan1
        } else {
            productPlan2
        }
    }

    override fun open(): Scan {
        return bestPlan.open()
    }

    override fun blocksAccessed(): Int {
        return bestPlan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return bestPlan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return bestPlan.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return bestPlan.schema()
    }
}