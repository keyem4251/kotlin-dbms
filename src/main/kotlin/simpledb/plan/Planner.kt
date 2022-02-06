package simpledb.plan

import simpledb.parse.*
import simpledb.tx.Transaction

class Planner(
    private val queryPlanner: QueryPlanner,
    private val updatePlanner: UpdatePlanner,
) {
    fun createQueryPlan(cmd: String, transaction: Transaction): Plan {
        val parser = Parser(cmd)
        val queryData = parser.query()
        // code to verify the query should be here...
        return queryPlanner.createPlan(queryData, transaction)
    }

    fun executeUpdate(cmd: String, transaction: Transaction): Int {
        val parser = Parser(cmd)
        // code to verify the update command should be here...
        return when (val any = parser.updateCmd()) {
            is InsertData -> {
                updatePlanner.executeInsert(any, transaction)
            }
            is DeleteData -> {
                updatePlanner.executeDelete(any, transaction)
            }
            is ModifyData -> {
                updatePlanner.executeModify(any, transaction)
            }
            is CreateTableData -> {
                updatePlanner.executeCreateTable(any, transaction)
            }
            is CreateViewData -> {
                updatePlanner.executeCreateView(any, transaction)
            }
            is CreateIndexData -> {
                updatePlanner.executeCreateIndex(any, transaction)
            }
            else -> {
                0
            }
        }
    }
}