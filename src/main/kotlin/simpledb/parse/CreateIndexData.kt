package simpledb.parse

data class CreateIndexData(
    private val indexName: String,
    private val tableName: String,
    private val fieldName: String,
)