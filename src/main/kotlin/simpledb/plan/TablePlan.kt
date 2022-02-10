package simpledb.plan

import simpledb.metadata.MetadataManager
import simpledb.metadata.StatisticsInformation
import simpledb.query.Scan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

/**
 * 保存されたテーブルのコストをテーブルのメタデータマネージャを用いて推定する
 *
 * @property tableName クエリの中の特定のテーブル
 */
class TablePlan(
    private val transaction: Transaction,
    private val tableName: String,
    private val metadataManager: MetadataManager,
) : Plan {
    private var layout: Layout = metadataManager.getLayout(tableName, transaction)
    private var statisticsInformation: StatisticsInformation =
        metadataManager.getStatisticsInformation(tableName, layout, transaction)

    /**
     * テーブルを読み取るためのTableScanクラスを返す
     */
    override fun open(): Scan {
        return TableScan(transaction, tableName, layout)
    }

    /**
     * テーブルの統計情報からブロックのアクセスの回数を推定する
     */
    override fun blocksAccessed(): Int {
        return statisticsInformation.blockAccessed()
    }

    /**
     * テーブルの統計情報からテーブルのレコード数を推定する
     */
    override fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput()
    }

    /**
     * テーブルの統計情報から指定されたフィールド名の数を推定する
     */
    override fun distinctValues(fieldName: String): Int {
        return statisticsInformation.distinctValues(fieldName)
    }

    /**
     * テーブルのスキーマを返す
     */
    override fun schema(): Schema {
        return layout.schema()
    }
}