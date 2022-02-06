package simpledb.parse

import simpledb.query.Predicate

/**
 * Selectを行うのに必要な値を格納するクラス
 * <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
 */
class QueryData(
    val fields: List<String>,
    val tables: Collection<String>,
    val predicate: Predicate,
) {
    override fun toString(): String {
        var result = "select "
        for (filedName in fields) {
            result += "$filedName, "
        }
        result = result.substring(0, result.length-2) //zap final comma
        result += " from "
        for (tableName in tables) {
            result += "$tableName, "
        }
        result = result.substring(0, result.length-2) // zap final comma
        val predicateString = predicate.toString()
        if (predicateString != "") {
            result += " where $predicateString"
        }
        return result
    }
}