package simpledb.parse

import simpledb.record.Schema

data class CreateTableData(
    private val tableName: String,
    private val schema: Schema,
)