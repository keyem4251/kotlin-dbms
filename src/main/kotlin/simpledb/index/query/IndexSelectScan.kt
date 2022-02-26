package simpledb.index.query

import simpledb.index.Index
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.TableScan

/**
 * Selectを行う際にindexをもとにSelect対象のテーブルを走査するためのクラス
 *
 * @property tableScan selec対象のテーブルのためのScan
 */
class IndexSelectScan(
    private val tableScan: TableScan,
    private val index: Index,
    private val value: Constant,
) : Scan {
    init {
        beforeFirst()
    }

    /**
     * Indexの最初のレコードの前の位置に移動する
     */
    override fun beforeFirst() {
        index.beforeFirst(value)
    }

    /**
     * 次のレコードに移動する
     * 検索条件を満たす次のレコードに移動を行いtrueを返す
     * 条件を満たすレコードがIndexにない場合falseを返す
     */
    override fun next(): Boolean {
        val ok = index.next()
        if (ok) {
            val rid = index.getDataRid()
            tableScan.moveToRid(rid)
        }
        return ok
    }

    /**
     * 現在の行の数値を返す
     */
    override fun getInt(fieldName: String): Int {
        return tableScan.getInt(fieldName)
    }

    /**
     * 現在の行の文字列を返す
     */
    override fun getString(fieldName: String): String {
        return tableScan.getString(fieldName)
    }

    /**
     * 現在の行のConstantの値を返す
     */
    override fun getVal(fieldName: String): Constant {
        return tableScan.getVal(fieldName)
    }

    /**
     * 指定されたフィールド[fieldName]を持つかを判定する
     */
    override fun hasField(fieldName: String): Boolean {
        return tableScan.hasField(fieldName)
    }

    /**
     * 検索を閉じる
     */
    override fun close() {
        index.close()
        tableScan.close()
    }
}