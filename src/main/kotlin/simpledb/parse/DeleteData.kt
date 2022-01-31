package simpledb.parse

import simpledb.query.Predicate

data class DeleteData(
    private val tableName: String,
    private val predicate: Predicate,
)