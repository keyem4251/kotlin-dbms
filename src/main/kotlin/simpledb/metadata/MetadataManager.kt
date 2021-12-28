package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction

class MetadataManager(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    companion object {
        lateinit var tableManager: TableManager
        lateinit var viewManager: ViewManager
        lateinit var statisticsManager: StatisticsManager
        lateinit var indexManager: IndexManager
    }

    init {
        tableManager = TableManager(isNew, transaction)
        viewManager = ViewManager(isNew, tableManager, transaction)
        statisticsManager = StatisticsManager(tableManager, transaction)
        indexManager = IndexManager(isNew, tableManager, statisticsManager, transaction)s)
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        tableManager.createTable(tableName, schema, tx)
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        return tableManager.getLayout(tableName, tx)
    }

    fun createView(viewName: String, viewDef: String, tx: Transaction) {
        viewManager.createView(viewName, viewDef, tx)
    }

    fun getViewDef(viewName: String, tx: Transaction): String {
        return viewManager.getViewDef(viewName, tx) ?: throw RuntimeException("viewdef null error")
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        indexManager.createIndex(indexName, tableName, fieldName, tx)
    }

    // returnのMutableMapをMapに変更できないか？
    fun getIndexInformation(tableName: String, tx: Transaction): MutableMap<String, IndexInfo> {
        return indexManager.getIndexInfo(tableName, tx)
    }

    fun getStatisticsInformation(tableName: String, layout: Layout, tx: Transaction): StatisticsInformation {
        return statisticsManager.getStatisticsInformation(tableName, layout, tx)
    }
}