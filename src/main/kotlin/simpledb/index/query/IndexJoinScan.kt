package simpledb.index.query

import simpledb.index.Index
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.record.TableScan

class IndexJoinScan(
    private val leftSideScan: Scan,
    private val index: Index,
    private val joinField: String,
    private val rightSideScan: TableScan,
) : Scan {
    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        leftSideScan.beforeFirst()
        leftSideScan.next()
        resetIndex()
    }

    override fun next(): Boolean {
        while (true) {
            if (index.next()) {
                rightSideScan.moveToRid(index.getDataRid())
                return true
            }
            if (!leftSideScan.next()) return false
            resetIndex()
        }
    }

    override fun getInt(fieldName: String): Int {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getInt(fieldName)
        } else {
            leftSideScan.getInt(fieldName)
        }
    }

    override fun getVal(fieldName: String): Constant {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getVal(fieldName)
        } else {
            leftSideScan.getVal(fieldName)
        }
    }

    override fun getString(fieldName: String): String {
        return if (rightSideScan.hasField(fieldName)) {
            rightSideScan.getString(fieldName)
        } else {
            leftSideScan.getString(fieldName)
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return rightSideScan.hasField(fieldName) || leftSideScan.hasField(fieldName)
    }

    override fun close() {
        leftSideScan.close()
        index.close()
        rightSideScan.close()
    }

    private fun resetIndex() {
        val searchKey = leftSideScan.getVal(joinField)
        index.beforeFirst(searchKey)
    }
}