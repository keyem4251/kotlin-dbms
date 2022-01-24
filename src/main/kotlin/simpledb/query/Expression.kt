package simpledb.query

import simpledb.record.Schema

/**
 * Constant、フィールド名のどちらかで成り立つ式を表すクラス
 * 「SName = "joe" and MajorId = DId」という条件式の場合、
 *  Expressionは以下のようになる
 *  左の式: SName フィールド名
 *  右の式: "joe" Constant（文字列）
 */
class Expression {
    private var value: Constant? = null
    private var fieldName: String? = ""

    constructor(value: Constant) {
        this.value = value
    }

    constructor(fieldName: String) {
        this.fieldName = fieldName
    }

    /**
     * Expressionクラスが条件式のフィールド名なのか、条件となる値のどちらの役割を持つのかを判定する
     * @return 比較される列場合true、条件となる値の場合false
     */
    fun isFieldName(): Boolean {
        return fieldName != null
    }

    fun asConstant(): Constant {
        return value  ?: throw RuntimeException("null error")
    }

    fun asFieldName(): String {
        return fieldName ?: throw RuntimeException("null error")
    }

    /**
     * Expressionクラスが比較される列の場合、テーブルの値を返し、
     * 条件となる値の場合はそのまま値を帰す
     * @return 条件、あるいはテーブルの値
     */
    fun evaluate(scan: Scan): Constant {
        return if (value != null) {
            // Expressionクラスが検索の条件となる値の場合
            value!!
        } else if (fieldName != null) {
            // Expressionクラス比較される列の場合、scanを通してテーブルの値を返す
            scan.getVal(fieldName!!)
        } else {
            throw RuntimeException("null error")
        }
    }

    fun appliesTo(schema: Schema): Boolean {
        return if (value != null) {
            true
        } else if (fieldName != null) {
            schema.hasField(fieldName!!)
        } else {
            throw RuntimeException("null error")
        }
    }

    override fun toString(): String {
        return if (value != null) {
            value!!.toString()
        } else if (fieldName != null) {
            fieldName!!
        } else {
            throw RuntimeException("null error")
        }
    }
}
