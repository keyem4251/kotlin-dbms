package simpledb.plan

import simpledb.query.ProjectScan
import simpledb.query.Scan
import simpledb.record.Schema

/**
 * 受け取ったplanで扱われているテーブルから、指定された[fieldList]フィールドを取り出すコストを見積もるクラス
 */
class ProjectPlan(
    private val plan: Plan,
    private val fieldList: List<String>,
) : Plan {
    private val schema = Schema()

    /**
     * 受け取ったplanのスキーマ、フィールド名からから自身のスキーマを作成する
     */
    init {
        for (fieldName in fieldList) {
            schema.add(fieldName, plan.schema())
        }
    }

    /**
     * テーブルのフィールドを返すためのProjectScanクラスを返す
     */
    override fun open(): Scan {
        val scan = plan.open()
        return ProjectScan(scan, schema.fields)
    }

    /**
     * テーブルのフィールドを返すためのブロックアクセスの回数を推定する
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * テーブルのフィールドを返すための行の数を推定する
     */
    override fun recordsOutput(): Int {
        return plan.recordsOutput()
    }

    /**
     * テーブルのフィールドを返す場合の[fieldName]指定されたフィールド名のばらつきをを推定する
     */
    override fun distinctValues(fieldName: String): Int {
        return plan.distinctValues(fieldName)
    }

    /**
     * 自身のスキーマを返す
     */
    override fun schema(): Schema {
        return schema
    }
}