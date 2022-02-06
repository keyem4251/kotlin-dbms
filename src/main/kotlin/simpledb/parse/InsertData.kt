package simpledb.parse

import simpledb.query.Constant

/**
 * Insertを行うのに必要な値を格納するクラス
 * <Insert> := INSERT INTO IdToken ( <FieldList> ) VALUES ( <ConstList> )
 */
data class InsertData(
    val tableName: String,
    val fields: List<String>,
    val values: List<Constant>,
)