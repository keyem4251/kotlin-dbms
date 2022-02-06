package simpledb.parse

/**
 * Indexを作成するのに必要な値を格納するクラス
 * <CreateIndex> := CREATE INDEX IdToken ON IdToken ( <Field> )
 */
data class CreateIndexData(
    val indexName: String,
    val tableName: String,
    val fieldName: String,
)