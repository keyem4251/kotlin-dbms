package simpledb.index.planner

import simpledb.metadata.MetadataManager
import simpledb.parse.*
import simpledb.plan.Plan
import simpledb.plan.SelectPlan
import simpledb.plan.TablePlan
import simpledb.plan.UpdatePlanner
import simpledb.query.UpdateScan
import simpledb.tx.Transaction

/**
 * テーブルの行のUpdateに伴いIndexのUpdateも行う場合のPlanクラス
 */
class IndexUpdatePlanner(
    private val metadataManager: MetadataManager,
) : UpdatePlanner {
    /**
     * Insertを行うデータをもとにUpdateScanでテーブルの更新を行う
     * 関連するIndexがある場合にIndexにテーブルのRID、insertする値を挿入する
     */
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

    /**
     * Deleteを行うデータをもとにIndexがある場合にIndexの値を削除する
     * その後テーブルの行を削除する
     */
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

    /**
     * Updateを行うデータをもとにUpdateScanでテーブルの更新を行う
     * 関連するIndexがある場合にIndexのテーブルの古い行を削除し、新しい行を挿入する（テーブルの値の更新に伴いIndex上に保存している値が変わるため）
     */
    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        val tableName = data.tableName
        val fieldName = data.fieldName
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val indexInfo = metadataManager.getIndexInformation(tableName, transaction)[fieldName]
        val index = indexInfo?.open()
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            // first, update the record
            val newValue = data.newValue.evaluate(updateScan)
            val oldValue = updateScan.getVal(fieldName)
            updateScan.setVal(data.fieldName, newValue)

            // then update the appropriate index, if it exists
            if (index != null) {
                val rid = updateScan.getRid()
                index.delete(oldValue, rid)
                index.insert(newValue, rid)
            }
            count += 1
        }
        index?.close()
        updateScan.close()
        return count
    }

    /**
     * レコードを操作しないためScanクラスは呼ばずに、メタデータを操作しテーブルを作成する
     */
    override fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int {
        metadataManager.createTable(data.tableName, data.schema, transaction)
        return 0
    }

    /**
     * レコードを操作しないためScanクラスは呼ばずに、メタデータを操作しビューを作成する
     */
    override fun executeCreateView(data: CreateViewData, transaction: Transaction): Int {
        metadataManager.createView(data.viewName, data.viewDef(), transaction)
        return 0
    }

    /**
     * レコードを操作しないためScanクラスは呼ばずに、メタデータを操作しインデックスを作成する
     */
    override fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int {
        metadataManager.createIndex(data.indexName, data.tableName, data.fieldName, transaction)
        return 0
    }
}