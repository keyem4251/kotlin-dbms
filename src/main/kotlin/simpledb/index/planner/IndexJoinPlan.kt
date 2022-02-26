package simpledb.index.planner

import simpledb.index.query.IndexJoinScan
import simpledb.metadata.IndexInfo
import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.record.TableScan

/**
 * 2つのテーブルとjoinを行うためのフィールドをもとにjoinする際にindexを使用する場合のコストを計算するためのクラス
 *
 * @property plan1 Table1
 * @property plan2 Table2
 * @property joinField Table1とTable2をjoinする際に使用するフィールド
 */
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

    /**
     * IndexJoinScanを作成する
     */
    override fun open(): Scan {
        val scan = plan1.open()
        // throws an exception if plan2 is not a table plan
        val tableScan = plan2.open() as TableScan
        val index = indexInfo.open()
        return IndexJoinScan(scan, index, joinField, tableScan)
    }

    /**
     * ブロックアクセスの数を推定する
     * 1つめのテーブルのブロックアクセス数 + (1つめのテーブルの行数 * Indexのブロックアクセスの数) + Indexの行数
     */
    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + (plan1.recordsOutput() * indexInfo.blocksAccessed()) + recordsOutput()
    }

    /**
     * JOIN際のレコードの出力される数
     * 1つめのテーブルのレコード数 * Indexのレコードの数
     */
    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * indexInfo.recordsOutput()
    }

    /**
     * 受け取ったフィールド[fieldName]のばらつきを返す
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    /**
     * 2つのテーブルを合わせたスキーマの情報を返す
     */
    override fun schema(): Schema {
        return schema
    }
}