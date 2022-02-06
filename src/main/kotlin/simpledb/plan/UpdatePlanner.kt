package simpledb.plan

import simpledb.parse.*
import simpledb.tx.Transaction

interface UpdatePlanner {
    fun executeInsert(data: InsertData, transaction: Transaction): Int

    fun executeDelete(data: DeleteData, transaction: Transaction): Int

    fun executeModify(data: ModifyData, transaction: Transaction): Int

    fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int

    fun executeCreateView(data: CreateViewData, transaction: Transaction): Int

    fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int
}