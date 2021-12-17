package simpledb.query

import java.lang.RuntimeException

class Constant: Comparable<Constant> {
    private var intValue: Int? = null
    private var stringValue: String? = null

    constructor(intValue: Int) {
        this.intValue = intValue
    }

    constructor(stringValue: String) {
        this.stringValue = stringValue
    }

    fun asInt(): Int? {
        return intValue
    }

    fun asString(): String? {
        return stringValue
    }

    override fun equals(other: Any?): Boolean {
        val c = other as Constant
        return if(intValue != null) intValue == c.intValue else stringValue.equals(c.stringValue)
    }

    override fun compareTo(other: Constant): Int {
        if (intValue != null && other.intValue != null) {
            return  intValue!!.compareTo(other.intValue!!)
        } else if (stringValue != null && other.stringValue != null) {
            return stringValue!!.compareTo(other.stringValue!!)
        }
        throw RuntimeException("compare error")
    }

    override fun hashCode(): Int {
        return if(intValue != null) intValue.hashCode() else stringValue.hashCode()
    }

    override fun toString(): String {
        return if(intValue != null) intValue.toString() else stringValue.toString()
    }
}