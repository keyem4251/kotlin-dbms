package simpledb.plan

import simpledb.query.Scan
import simpledb.record.Schema

interface Plan {
    fun open(): Scan

    fun blocksAccessed(): Int

    fun recordsOutput(): Int

    fun distinctValues(fieldName: String): Int

    fun schema(): Schema
}
