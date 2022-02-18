package simpledb.index.planner

import simpledb.index.query.IndexJoinScan
import simpledb.metadata.IndexInfo
import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.record.TableScan

class IndexJoinPlan(
    private val plan1: Plan,
    private val plan2: Plan,
    private val joinField: String,
    private val indexInfo: IndexInfo,
) : Plan {
    private val schema = Schema()

    init {
        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    override fun open(): Scan {
        val scan = plan1.open()
        // throws an exception if plan2 is not a table plan
        val tableScan = plan2.open() as TableScan
        val index = indexInfo.open()
        return IndexJoinScan(scan, index, joinField, tableScan)
    }

    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + (plan1.recordsOutput() * indexInfo.blocksAccessed()) + recordsOutput()
    }

    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * indexInfo.recordsOutput()
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