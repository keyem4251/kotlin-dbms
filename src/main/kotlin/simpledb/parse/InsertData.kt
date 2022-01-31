package simpledb.parse

import simpledb.query.Constant

data class InsertData(
    private val tableName: String,
    private val fields: List<String>,
    private val values: List<Constant>,
)