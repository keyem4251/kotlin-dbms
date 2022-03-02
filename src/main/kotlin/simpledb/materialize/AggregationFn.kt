package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

interface AggregationFn {
    fun processFirst(scan: Scan)
    fun processNext(scan: Scan)
    fun fieldName(): String
    fun value(): Constant
}