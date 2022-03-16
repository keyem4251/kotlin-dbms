package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.tx.Transaction

/**
 * merge joinを行うプラン（indexを用いたjoinではなく）
 *
 * @property plan1 入力となるテーブル1
 * @property plan2 入力となるテーブル2 ソートされている
 * @property fieldName1 テーブル1の結合を行うフィールド1
 * @property fieldName2 テーブル2の結合を行うフィールド2
 */
class MergeJoinPlan(
    private val transaction: Transaction,
    private var plan1: Plan,
    private var plan2: Plan,
    private val fieldName1: String,
    private val fieldName2: String,
) : Plan {
    private val schema = Schema()

    /**
     * 2つのテーブルを結合するフィールドでsortしSortPlanを作成する
     */
    init {
        val sortList1 = mutableListOf<String>(fieldName1)
        plan1 = SortPlan(plan1, transaction, sortList1)

        val sortList2 = mutableListOf<String>(fieldName2)
        plan2 = SortPlan(plan2, transaction, sortList2)

        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    /**
     * sortされた2つのテーブルからMergeJoinScanを作成する
     */
    override fun open(): Scan {
        val scan: Scan = plan1.open()
        val sortScan: SortScan = plan2.open() as SortScan
        return MergeJoinScan(scan, sortScan, fieldName1, fieldName2)
    }

    /**
     * ソートされたテーブルからブロックアクセスの回数を推定し返す
     * それぞれのテーブルのブロックアクセスの数を足したものになる
     */
    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + plan2.blocksAccessed()
    }

    /**
     * merge joinを行う際の2つのテーブルをもとにレコードの総数を推定する
     */
    override fun recordsOutput(): Int {
        val maxValues = plan1.distinctValues(fieldName1).coerceAtLeast(plan2.distinctValues(fieldName2))
        return (plan1.recordsOutput() * plan2.recordsOutput()) / maxValues
    }

    /**
     * フィールドのばらつきを返す
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    /**
     * スキーマを返す
     */
    override fun schema(): Schema {
        return schema
    }
}