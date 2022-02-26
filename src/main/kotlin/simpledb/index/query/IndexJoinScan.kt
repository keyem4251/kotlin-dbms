package simpledb.index.query

import simpledb.index.Index
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.TableScan

/**
 * 2つのテーブルとjoinを行うためのフィールドをもとにindexを走査する
 * ProductScanと似ている
 */
class IndexJoinScan(
    private val leftSideScan: Scan,
    private val index: Index,
    private val joinField: String,
    private val rightSideScan: TableScan,
) : Scan {
    init {
        beforeFirst()
    }

    /**
     * 最初のレコードの前に移動する
     * 1つめのテーブルの移動を行い、resetIndexで検索キーを取得しindexの行も移動を行う
     */
    override fun beforeFirst() {
        leftSideScan.beforeFirst()
        leftSideScan.next()
        resetIndex()
    }

    /**
     * 次のレコードに移動する
     * 条件を満たすIndexの次の行に移動を行う
     * Indexの行がなければ、1つめのテーブルをnextで進め、indexをリセットする
     * 1つめのテーブルの次の行がなければfalseを返す
     */
    override fun next(): Boolean {
        while (true) {
            if (index.next()) {
                rightSideScan.moveToRid(index.getDataRid())
                return true
            }
            if (!leftSideScan.next()) return false
            resetIndex()
        }
    }

    /**
     * 現在の行の数値を返す
     */
    override fun getInt(fieldName: String): Int {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getInt(fieldName)
        } else {
            leftSideScan.getInt(fieldName)
        }
    }

    /**
     * 現在の行のConstant値を返す
     */
    override fun getVal(fieldName: String): Constant {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getVal(fieldName)
        } else {
            leftSideScan.getVal(fieldName)
        }
    }

    /**
     * 現在の行の文字列を返す
     */
    override fun getString(fieldName: String): String {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getString(fieldName)
        } else {
            leftSideScan.getString(fieldName)
        }
    }

    /**
     * 指定されたフィールド[fieldName]を持つかを判定する
     */
    override fun hasField(fieldName: String): Boolean {
        return rightSideScan.hasField(fieldName) || leftSideScan.hasField(fieldName)
    }

    /**
     * テーブル、Indexを閉じる
     */
    override fun close() {
        leftSideScan.close()
        index.close()
        rightSideScan.close()
    }

    /**
     * Joinする場合に1つめのテーブルの値を取得し、その値をもとにIndexの行を移動する
     */
    private fun resetIndex() {
        val searchKey = leftSideScan.getVal(joinField)
        index.beforeFirst(searchKey)
    }
}