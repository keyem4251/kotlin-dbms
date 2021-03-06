package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * 現在のグループを保持する
 *
 * @property scan sort scan
 * @property fields グルーピングを行うフィールド
 */
class GroupValue(
    private val scan: Scan,
    private val fields: List<String>,
) {
    private val values = mutableMapOf<String, Constant>()

    init {
        for (fieldName in fields) {
            values[fieldName] = scan.getVal(fieldName)
        }
    }

    fun getVal(fieldName: String): Constant {
        return values[fieldName] ?: throw RuntimeException("null error")
    }

    /**
     * 2つのGroup Valueが持つフィールドの値が同じかを判定する
     */
    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        val groupValue = other as GroupValue
        for (fieldName in values.keys) {
            val value1 = values[fieldName]
            val value2 = groupValue.getVal(fieldName)
            if (!value1!!.equals(value2)) {
                return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        var hashValue = 0
        for (constant in values.values) {
            hashValue += constant.hashCode()
        }
        return hashValue
    }
}