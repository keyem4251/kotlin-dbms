package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

class MaxFn(
    private val fieldName: String
) : AggregationFn {
    private lateinit var value: Constant

    override fun processFirst(scan: Scan) {
        value = scan.getVal(fieldName)
    }

    override fun processNext(scan: Scan) {
        val newValue = scan.getVal(fieldName)
        if (newValue.compareTo(value) > 0) {
            value = newValue
        }
    }

    override fun fieldName(): String {
        return "maxof$fieldName"
    }

    override fun value(): Constant {
        return value
    }
}