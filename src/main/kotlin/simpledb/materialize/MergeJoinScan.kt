package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

class MergeJoinScan(
    private val scan: Scan,
    private val sortScan: SortScan,
    private val fieldName1: String,
    private val fieldName2: String,
) : Scan {
    private var joinValue: Constant? = null

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan.beforeFirst()
        sortScan.beforeFirst()
    }

    override fun close() {
        scan.close()
        sortScan.close()
    }

    override fun next(): Boolean {
        var hasMore2 = sortScan.next()
        if (hasMore2 && sortScan.getVal(fieldName2).equals(joinValue)) return true

        var hasMore1 = scan.next()
        if (hasMore1 && scan.getVal(fieldName1).equals(joinValue)) {
            sortScan.restorePosition()
            return true
        }

        while (hasMore1 && hasMore2) {
            val value1 = scan.getVal(fieldName1)
            val value2 = sortScan.getVal(fieldName2)
            if (value1.compareTo(value2) < 0) {
                hasMore1 = scan.next()
            } else if (value1.compareTo(value2) > 0) {
                hasMore2 = sortScan.next()
            } else {
                sortScan.savePosition()
                joinValue = sortScan.getVal(fieldName2)
                return true
            }
        }
        return false
    }

    override fun getInt(fieldName: String): Int {
        return if (scan.hasField(fieldName)) {
            scan.getInt(fieldName)
        } else {
            sortScan.getInt(fieldName)
        }
    }

    override fun getString(fieldName: String): String {
        return if (scan.hasField(fieldName)) {
            scan.getString(fieldName)
        } else {
            sortScan.getString(fieldName)
        }
    }

    override fun getVal(fieldName: String): Constant {
        return if (scan.hasField(fieldName)) {
            scan.getVal(fieldName)
        } else {
            sortScan.getVal(fieldName)
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return scan.hasField(fieldName) || sortScan.hasField(fieldName)
    }
}