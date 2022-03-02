package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

class GroupByScan(
    private val scan: Scan,
    private val groupFields: List<String>,
    private val aggregationFns: List<AggregationFn>,
) : Scan {
    private lateinit var groupValue: GroupValue
    private var moreGroups = false

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan.beforeFirst()
        moreGroups = scan.next()
    }

    override fun next(): Boolean {
        if (!moreGroups) return false
        for (aggregationFn in aggregationFns) {
            aggregationFn.processFirst(scan)
        }
        groupValue = GroupValue(scan, groupFields)
        moreGroups = scan.next()
        while (moreGroups) {
            val nextGroupValue = GroupValue(scan, groupFields)
            if (!groupValue.equals(nextGroupValue)) {
                break
            }
            for (aggregationFn in aggregationFns) {
                aggregationFn.processNext(scan)
            }
            moreGroups = scan.next()
        }
        return true
    }

    override fun close() {
        scan.close()
    }

    override fun getVal(fieldName: String): Constant {
        if (groupFields.contains(fieldName)) return groupValue.getVal(fieldName)
        for (aggregationFn in aggregationFns) {
            if (aggregationFn.fieldName() == fieldName) return aggregationFn.value()
        }
        throw RuntimeException("no field $fieldName")
    }

    override fun getInt(fieldName: String): Int {
        return getVal(fieldName).asInt()!!
    }

    override fun getString(fieldName: String): String {
        return getVal(fieldName).asString()!!
    }

    override fun hasField(fieldName: String): Boolean {
        if (groupFields.contains(fieldName)) return true
        for (aggregationFn in aggregationFns) {
            if (aggregationFn.fieldName() == fieldName) return true
        }
        return false
    }
}