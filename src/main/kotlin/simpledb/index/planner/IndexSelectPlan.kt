package simpledb.index.planner

import simpledb.index.query.IndexSelectScan
import simpledb.metadata.IndexInfo
import simpledb.plan.Plan
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.Schema
import simpledb.record.TableScan

/**
 * Selectする場合にindexを使用するプラン情報を扱うクラス
 *
 * @property plan Table Planクラス
 * @property indexInfo Indexの情報
 * @property value 検索キー（Indexで検索する対象）
 */
class IndexSelectPlan(
    private val plan: Plan,
    private val indexInfo: IndexInfo,
    private val value: Constant,
) : Plan {
    /**
     * Indexテーブルを走査するためのScanクラスを作成、返す
     */
    override fun open(): Scan {
        // throws an exception if p is not a table plan.
        val tableScan = plan.open() as TableScan
        val index = indexInfo.open()
        return IndexSelectScan(tableScan, index, value)
    }

    /**
     * Indexを走査する際のブロックアクセスの数を推定する
     * Indexの走査コストに合致するレコード数を足したもの
     */
    override fun blocksAccessed(): Int {
        return indexInfo.blocksAccessed() + recordsOutput()
    }

    /**
     * Indexを選択した際の出力されるレコード数を推定する
     * Indexの検索キーの数を一致する
     */
    override fun recordsOutput(): Int {
        return indexInfo.recordsOutput()
    }

    /**
     * [fieldName]指定されたフィールドのindexのばらつきを返す
     */
    override fun distinctValues(fieldName: String): Int {
        return indexInfo.distinctValues(fieldName)
    }

    /**
     * スキーマ情報を返す
     */
    override fun schema(): Schema {
        return plan.schema()
    }
}