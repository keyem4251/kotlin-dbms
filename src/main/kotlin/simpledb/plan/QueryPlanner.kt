package simpledb.plan

import simpledb.parse.QueryData
import simpledb.tx.Transaction

/**
 * 参照系のクエリプランナーのインターフェイス
 */
interface QueryPlanner {
    fun createPlan(data: QueryData, transaction: Transaction): Plan
}