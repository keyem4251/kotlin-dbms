package simpledb.parse

import simpledb.query.Predicate

/**
 * Deleteを行うのに必要な値を格納するクラス
 * <Delete> := DELETE FROM IdToken [ WHERE <Predicate> ]
 */
data class DeleteData(
    val tableName: String,
    val predicate: Predicate,
)