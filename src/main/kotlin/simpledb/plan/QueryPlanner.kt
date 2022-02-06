package simpledb.plan

import simpledb.parse.QueryData
import simpledb.tx.Transaction

interface QueryPlanner {
    fun createPlan(data: QueryData, transaction: Transaction): Plan
}