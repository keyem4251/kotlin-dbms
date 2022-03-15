package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * グループ内のレコードの数を返す
 */
class CountFn(
    private val fieldName: String,
) : AggregationFn {
    private var count: Int = 0

    /**
     * グループの数を1として設定する
     */
    override fun processFirst(scan: Scan) {
        count = 1
    }

    /**
     * グループの数に1を足す
     */
    override fun processNext(scan: Scan) {
        count += 1
    }

    /**
     * フィールド名を返す
     */
    override fun fieldName(): String {
        return "countof$fieldName"
    }

    /**
     * グループ内のレコードの数を返す
     */
    override fun value(): Constant {
        return Constant(count)
    }
}