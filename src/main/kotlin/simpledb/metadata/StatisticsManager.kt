package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.TableScan
import simpledb.tx.Transaction

class StatisticsManager(
    private val tableManager: TableManager,
    private val tx: Transaction,
) {
    private val tableStatistics = mutableMapOf<String, StatisticsInformation>()
    private var numberCalls = 0


    init {
        refreshStatistics(tx)
    }

    @Synchronized
    fun getStatisticsInformation(tableName: String, layout: Layout, transaction: Transaction): StatisticsInformation {
        numberCalls += 1
        if (numberCalls > 100) refreshStatistics(transaction)
        var statisticsInformation = tableStatistics[tableName]
        if (statisticsInformation == null) {
            statisticsInformation = calcTableStatistics(tableName, layout, transaction)
            tableStatistics[tableName] = statisticsInformation
        }
        return statisticsInformation
    }

    @Synchronized
    private fun refreshStatistics(transaction: Transaction) {
        numberCalls = 0
        val tableCatalogLayout = tableManager.getLayout("tablecatalog", transaction)
        val tableCatalog = TableScan(transaction, "tablecatalog", tableCatalogLayout)
        while (tableCatalog.next()) {
            val tableName = tableCatalog.getString("tablename")
            val layout = tableManager.getLayout(tableName, transaction)
            val statisticsInformation = calcTableStatistics(tableName, layout, transaction)
            tableStatistics[tableName] = statisticsInformation
        }
        tableCatalog.close()
    }

    @Synchronized
    private fun calcTableStatistics(tableName: String, layout: Layout, transaction: Transaction): StatisticsInformation {
        var numberRecords = 0
        var numberBlocks = 0
        val tableScan = TableScan(transaction, tableName, layout)
        while (tableScan.next()) {
            numberRecords += 1
            numberBlocks = tableScan.getRid().blockNumber + 1
        }
        tableScan.close()
        return StatisticsInformation(numberBlocks, numberRecords)
    }
}