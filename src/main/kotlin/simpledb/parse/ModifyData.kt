package simpledb.parse

import simpledb.query.Expression
import simpledb.query.Predicate

class ModifyData(
    private val tableName: String,
    private val fieldName: String,
    private val newValue: Expression,
    private val predicate: Predicate,
)