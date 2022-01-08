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
     * インデックスのスキーマ情報からブロックのレコードの数を計算し、インデックスに関連するテーブルの統計情報からブロックの数を計算し
     * インデックスクラスに渡すことで検索のコストを返す
     * @return ブロックアクセス数
     */
    fun blocksAccessed(): Int {
        val recordPerBlock = transaction.blockSize() / indexLayout.slotSize()
        val numberBlocks = statisticsInformation.recordsOutput() / recordPerBlock
        return HashIndex.searchCost(numberBlocks, recordPerBlock)
    }

    /**
     * 関連するテーブルのレコードの中でインデックスが張られているレコード数を推定して返す
     * @return インデックスのレコード数
     */
    fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput() / statisticsInformation.distinctValues(fieldName)
    }

    /**
     * [fName]指定されたフィールドが関連するテーブルのインデックスされたフィールドなら1を返し、
     * 異なるフィールドの場合は統計情報からばらつきを計算する
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
     * スキーマはdataRID（ブロック番号とレコードIDの整数）とdataVal（インデックスされたフィールド）で構成されている
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