package simpledb.query

import simpledb.record.Schema

class Expression {
    private var value: Constant? = null
    private var fieldName: String? = ""

    constructor(value: Constant) {
        this.value = value
    }

    constructor(fieldName: String) {
        this.fieldName = fieldName
    }

    fun isFieldName(): Boolean {
        return fieldName != null
    }

    fun asConstant(): Constant {
        return value  ?: throw RuntimeException("null error")
    }

    fun asFieldName(): String {
        return fieldName ?: throw RuntimeException("null error")
    }

    fun evaluate(scan: Scan): Constant {
        return if (value != null) {
            value!!
        } else if (fieldName != null) {
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

    fun toString(): String {
        return if (value != null) {
            value!!.toString()
        } else if (fieldName != null) {
            fieldName!!
        } else {
            throw RuntimeException("null error")
        }
    }
}
