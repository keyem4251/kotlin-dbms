package simpledb.plan

import simpledb.metadata.MetadataManager
import simpledb.metadata.StatisticsInformation
import simpledb.query.Scan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

class TablePlan(
    private val transaction: Transaction,
    private val tableName: String,
    private val metadataManager: MetadataManager,
) : Plan {
    private var layout: Layout = metadataManager.getLayout(tableName, transaction)
    private var statisticsInformation: StatisticsInformation =
        metadataManager.getStatisticsInformation(tableName, layout, transaction)

    override fun open(): Scan {
        return TableScan(transaction, tableName, layout)
    }

    override fun blocksAccessed(): Int {
        return statisticsInformation.blockAccessed()
    }

    override fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return statisticsInformation.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return layout.schema()
    }
}