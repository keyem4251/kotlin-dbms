package simpledb.metadata

import simpledb.index.Index
import simpledb.index.hash.HashIndex
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction

class IndexInfo(
    private val indexName: String,
    private val fieldName: String,
    private val transaction: Transaction,
    private val tableSchema: Schema,
    private val statisticsInformation: StatisticsInformation
) {
    private var indexLayout: Layout
    init {
        indexLayout = createIndexLayout()
    }

    fun open(): Index {
        return HashIndex(transaction, indexName, indexLayout)
    }

    fun blocksAccessed(): Int {
        val recordPerBlock = transaction.blockSize() / indexLayout.slotSize()
        val numberBlocks = statisticsInformation.recordsOutput() / recordPerBlock
        return HashIndex.searchCost(numberBlocks, recordPerBlock)
    }

    fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput() / statisticsInformation.distinctValues(fieldName)
    }

    fun distinctValues(fName: String): Int {
        return if (fieldName == fName) {
            1
        } else {
            statisticsInformation.distinctValues(fieldName)
        }
    }

    private fun createIndexLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        if (tableSchema.type(fieldName) == java.sql.Types.INTEGER) {
            schema.addIntField("dataval")
        } else {
            val fieldLength = tableSchema.length(fieldName) ?: throw RuntimeException("field length null error")
            schema.addStringField("datavale", fieldLength)
        }
        return Layout(schema)
    }
}