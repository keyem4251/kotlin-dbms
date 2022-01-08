package simpledb.metadata

import simpledb.index.Index
import simpledb.index.hash.HashIndex
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction

/**
 * インデックスに関する統計情報を持つクラス
 */
class IndexInfo(
    private val indexName: String,
    private val fieldName: String,
    private val transaction: Transaction,
    private val tableSchema: Schema,
    private val statisticsInformation: StatisticsInformation
) {
    private var indexLayout: Layout
    /**
     * コンストラクタで受け取るインデックスの情報、関連するテーブルの統計情報を元にインデックスのスキーマ情報を作成する
     * IndexInfoクラスがインデックスのスキーマ、インデックスのファイルサイズを見積もるためのレイアウトクラス
     */
    init {
        indexLayout = createIndexLayout()
    }

    /**
     * インデックスクラスを返す
     * @return インデックスクラス
     */
    fun open(): Index {
        return HashIndex(transaction, indexName, indexLayout)
    }

    /**
     * インデックスの検索に必要なブロックアクセス数を返す（インデックスのサイズではなく）
     * @return ブロックアクセス数
     */
    fun blocksAccessed(): Int {
        val recordPerBlock = transaction.blockSize() / indexLayout.slotSize()
        val numberBlocks = statisticsInformation.recordsOutput() / recordPerBlock
        return HashIndex.searchCost(numberBlocks, recordPerBlock)
    }

    /**
     * インデックスのレコード数を返す
     * @return インデックスのレコード数
     */
    fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput() / statisticsInformation.distinctValues(fieldName)
    }

    /**
     * インデックスのフィールドのばらつき（フィールドが何種類か）を返す
     * @return フィールドのばらつき
     */
    fun distinctValues(fName: String): Int {
        return if (fieldName == fName) {
            1
        } else {
            statisticsInformation.distinctValues(fieldName)
        }
    }

    /**
     * コンストラクタで受け取る情報を元にインデックスのスキーマ情報を作成する
     */
    private fun createIndexLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        if (tableSchema.type(fieldName) == java.sql.Types.INTEGER) {
            schema.addIntField("dataval")
        } else {
            val fieldLength = tableSchema.length(fieldName) ?: throw RuntimeException("field length null error")
            schema.addStringField("datavale", fieldLength)
        }
        return Layout(schema)
    }
}