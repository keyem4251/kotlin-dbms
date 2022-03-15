package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * グループの中の最大値を返す関数
 */
class MaxFn(
    private val fieldName: String
) : AggregationFn {
    private lateinit var value: Constant

    /**
     * 現在のレコードをグループの最大値として設定
     */
    override fun processFirst(scan: Scan) {
        value = scan.getVal(fieldName)
    }

    /**
     * 現在のレコードとグループの最大値として設定されている値を比較し、大きい方を設定する
     */
    override fun processNext(scan: Scan) {
        val newValue = scan.getVal(fieldName)
        if (newValue.compareTo(value) > 0) {
            value = newValue
        }
    }

    /**
     * フィールド名を返す
     */
    override fun fieldName(): String {
        return "maxof$fieldName"
    }

    /**
     * 最大値を返す
     */
    override fun value(): Constant {
        return value
    }
}