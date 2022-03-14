package simpledb.materialize

import simpledb.query.Scan

/**
 * scanのためのcomparator
 */
class RecordComparator(
    private val fields: Collection<String>
) : Comparator<Scan> {
    /**
     * 受け取った2つのscan（[o1], [o2]）の現在のレコードを比較する
     * レコードが異なる値を持つフィールドを保つ場合に、その値が比較の結果として使用される。
     * 2つのレコードがすべてのソートフィールドで同じ値を保つ場合には0を返す
     */
    override fun compare(o1: Scan?, o2: Scan?): Int {
        for (fieldName in fields) {
            val value1 = o1?.getVal(fieldName) ?: throw RuntimeException("null error")
            val value2 = o2?.getVal(fieldName) ?: throw RuntimeException("null error")
            val result = value1.compareTo(value2)
            if (result != 0) return result
        }
        return 0
    }
}