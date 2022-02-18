package simpledb.index.planner

import simpledb.metadata.MetadataManager
import simpledb.parse.DeleteData
import simpledb.parse.InsertData
import simpledb.plan.Plan
import simpledb.plan.SelectPlan
import simpledb.plan.TablePlan
import simpledb.plan.UpdatePlanner
import simpledb.query.UpdateScan
import simpledb.tx.Transaction

class IndexUpdatePlanner(
    private val metadataManager: MetadataManager,
) : UpdatePlanner {
    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val tableName = data.tableName
        val plan = TablePlan(transaction, tableName, metadataManager)

        // first, insert the record
        val updateScan = plan.open() as UpdateScan
        updateScan.insert()
        val rid = updateScan.getRid()

        // then modify each field, inserting index records
        val indexes = metadataManager.getIndexInformation(tableName, transaction)
        val valueIterator = data.values.iterator()
        for (fieldName in data.fields) {
            val value = valueIterator.next()
            println("Modify field $fieldName to val $value")
            updateScan.setVal(fieldName, value)

            val indexInfo = indexes[fieldName]
            if (indexInfo != null) {
                val index = indexInfo.open()
                index.insert(value, rid)
                index.close()
            }
        }
        updateScan.close()
        return 1
    }

    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        val tableName = data.tableName
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val indexes = metadataManager.getIndexInformation(tableName, transaction)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            // first, delete the record's RID from every index
            val rid = updateScan.getRid()
            for (fieldName in indexes.keys) {
                val value = updateScan.getVal(fieldName)
                val index = indexes[fieldName]?.open() ?: throw RuntimeException("null error")
                index.delete(value, rid)
                index.close()
            }
            // then delete the record
            updateScan.delete()
            count += 1
        }
        updateScan.close()
        return count
    }
}