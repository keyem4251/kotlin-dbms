package simpledb.plan

import simpledb.query.ProductScan
import simpledb.query.Scan
import simpledb.record.Schema

/**
 * 複数のプランを掛け合わせたコストを見積もるクラス
 */
class ProductPlan(
    private val plan1: Plan,
    private val plan2: Plan,
) : Plan {
    private val schema = Schema()

    /**
     * 受け取ったplanのスキーマから自身のスキーマを作成する
     */
    init {
        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    /**
     * 複数のテーブルを読み取るためのProductScanクラスを返す
     */
    override fun open(): Scan {
        val scan1 = plan1.open()
        val scan2 = plan2.open()
        return ProductScan(scan1, scan2)
    }

    /**
     * BlocksAccessed(product(p1, p2)) = BlocksAccessed(p1) + (RecordsOutput(p1) *  RecordsOutput(p2))
     * 複数のプランをかけ合わせた場合のブロックアクセスの回数を推定する
     */
    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + (plan1.recordsOutput() * plan2.recordsOutput())
    }

    /**
     * 複数のプランをかけ合わせた場合のレコード数を推定する
     */
    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * plan2.recordsOutput()
    }

    /**
     * 複数のプランをかけ合わせた場合の指定されたフィールド名の数を推定する
     * 複数のテーブルのかけ合わせは特定のフィールドのばらつきを増減しないため、元になるクエリの値と同じになる
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    /**
     * 複数のプランをかけ合わせたスキーマを返す
     */
    override fun schema(): Schema {
        return schema
    }
}