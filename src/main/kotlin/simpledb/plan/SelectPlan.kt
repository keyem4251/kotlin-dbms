package simpledb.plan

import simpledb.query.Predicate
import simpledb.query.Scan
import simpledb.query.SelectScan
import simpledb.record.Schema

/**
 * 条件式とプランを合わせたコストを見積もるクラス
 */
class SelectPlan(
    private val plan: Plan,
    private val predicate: Predicate,
) : Plan {

    /**
     * 条件式を満たす行を出力するためにSelectScanクラスを返す
     */
    override fun open(): Scan {
        val scan = plan.open()
        return SelectScan(scan, predicate)
    }

    /**
     * ブロックアクセスの回数を推定する
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * 条件式を満たす行を推定し返す
     */
    override fun recordsOutput(): Int {
        return plan.recordsOutput() / predicate.reductionFactor(plan)
    }

    /**
     * 条件式を満たす行のばらつきを推定し返す
     */
    override fun distinctValues(fieldName: String): Int {
        return if (predicate.equateWithConstant(fieldName) != null) {
            // [F=c]のcがfieldNameのためばらつきがなくなる
            1
        } else {
            // 条件式を考慮しないためクエリのフィールドのばらつきを返す
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