package simpledb.parse

import simpledb.record.Schema

/**
 * テーブルを作成するのに必要な値を格納するクラス
 * <CreateTable> := CREATE TABLE IdToken ( <FieldDefs> )
 */
data class CreateTableData(
    private val tableName: String,
    private val schema: Schema,
)