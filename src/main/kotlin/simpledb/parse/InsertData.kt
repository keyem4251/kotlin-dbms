package simpledb.parse

import simpledb.query.Constant

/**
 * Insertを行うのに必要な値を格納するクラス
 * <Insert> := INSERT INTO IdToken ( <FieldList> ) VALUES ( <ConstList> )
 */
data class InsertData(
    private val tableName: String,
    private val fields: List<String>,
    private val values: List<Constant>,
)