package simpledb.parse

/**
 * Indexを作成するのに必要な値を格納するクラス
 * <CreateIndex> := CREATE INDEX IdToken ON IdToken ( <Field> )
 */
data class CreateIndexData(
    private val indexName: String,
    private val tableName: String,
    private val fieldName: String,
)