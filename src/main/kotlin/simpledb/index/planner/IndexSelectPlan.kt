package simpledb.index.planner

import simpledb.index.query.IndexSelectScan
import simpledb.metadata.IndexInfo
import simpledb.plan.Plan
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.record.TableScan

class IndexSelectPlan(
    private val plan: Plan,
    private val indexInfo: IndexInfo,
    private val value: Constant,
) : Plan {
    override fun open(): Scan {
        // throws an exception if p is not a table plan.
        val tableScan = plan.open() as TableScan
        val index = indexInfo.open()
        return IndexSelectScan(tableScan, index, value)
    }

    override fun blocksAccessed(): Int {
        return indexInfo.blocksAccessed() + recordsOutput()
    }

    override fun recordsOutput(): Int {
        return indexInfo.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return indexInfo.distinctValues()
    }

    override fun schema(): Schema {
        return plan.schema()
    }
}