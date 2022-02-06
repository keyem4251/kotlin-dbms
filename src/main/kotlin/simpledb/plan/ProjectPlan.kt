package simpledb.plan

import simpledb.query.ProjectScan
import simpledb.query.Scan
import simpledb.record.Schema

class ProjectPlan(
    private val plan: Plan,
    private val fieldList: MutableList<String>,
) : Plan {
    private val schema = Schema()

    init {
        for (fieldName in fieldList) {
            schema.add(fieldName, plan.schema())
        }
    }

    override fun open(): Scan {
        val scan = plan.open()
        return ProjectScan(scan, schema.fields)
    }

    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return plan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return plan.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return schema
    }
}