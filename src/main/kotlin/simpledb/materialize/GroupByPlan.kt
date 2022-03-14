package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.tx.Transaction

/**
 * 入力となるテーブル、集約を行うフィールド、関数の3つをもとにする
 *
 * @property plan 入力レコードのために初期化時にsort planにする
 * @property groupFields group by を行うフィールド
 * @property aggregationFns 集約を行う関数
 */
class GroupByPlan(
    private val transaction: Transaction,
    private var plan: Plan,
    private val groupFields: List<String>,
    private val aggregationFns: List<AggregationFn>,
) : Plan {
    private val schema = Schema()

    /**
     * 集約を行うフィールドでsortを行う
     */
    init {
        plan = SortPlan(plan, transaction, groupFields)
        for (fieldName in groupFields) {
            schema.add(fieldName, plan.schema())
        }
        for (aggregationFn in aggregationFns) {
            schema.addIntField(aggregationFn.fieldName())
        }
    }

    /**
     * Group Byを行うためのscanを作成する
     */
    override fun open(): Scan {
        val scan: Scan = plan.open()
        return GroupByScan(scan, groupFields, aggregationFns)
    }

    /**
     * ブロックアクセスの数を推定し返す
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * グループ化された状態のレコードの数を推定し返す
     * 各フィールドのばらつきの積を計算する
     */
    override fun recordsOutput(): Int {
        var numberGroups = 1
        for (fieldName in groupFields) {
            numberGroups *= plan.distinctValues(fieldName)
        }
        return numberGroups
    }

    /**
     * レコードのばらつきを返す
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan.schema().hasField(fieldName)) {
            plan.distinctValues(fieldName)
        } else {
            recordsOutput()
        }
    }

    /**
     * スキーマを返す（集約するフィールドと集約する関数のフィールドを持つ）
     */
    override fun schema(): Schema {
        return schema
    }
}