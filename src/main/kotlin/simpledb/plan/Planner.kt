package simpledb.plan

import simpledb.parse.*
import simpledb.tx.Transaction

/**
 * SimpleDBがSQLのコストを取得するためのプランナークラス
 * 参照系、更新系のプランをそれぞれのメソッドで返す
 */
class Planner(
    private val queryPlanner: QueryPlanner,
    private val updatePlanner: UpdatePlanner,
) {
    /**
     * 参照系のSQLに対してプランを返す
     * parserでSQLを解釈し、queryDataとしてSQLをデータに格納しプランを返す
     */
    fun createQueryPlan(cmd: String, transaction: Transaction): Plan {
        val parser = Parser(cmd)
        val queryData = parser.query()
        // code to verify the query should be here...
        return queryPlanner.createPlan(queryData, transaction)
    }

    /**
     * 更新系のSQLに対してプランを返す
     * parserでSQLを解釈し、更新系の情報をupdateDateとして該当するプランを返す
     */
    fun executeUpdate(cmd: String, transaction: Transaction): Int {
        val parser = Parser(cmd)
        // code to verify the update command should be here...
        return when (val updateData = parser.updateCmd()) {
            is InsertData -> {
                updatePlanner.executeInsert(updateData, transaction)
            }
            is DeleteData -> {
                updatePlanner.executeDelete(updateData, transaction)
            }
            is ModifyData -> {
                updatePlanner.executeModify(updateData, transaction)
            }
            is CreateTableData -> {
                updatePlanner.executeCreateTable(updateData, transaction)
            }
            is CreateViewData -> {
                updatePlanner.executeCreateView(updateData, transaction)
            }
            is CreateIndexData -> {
                updatePlanner.executeCreateIndex(updateData, transaction)
            }
            else -> {
                0
            }
        }
    }
}