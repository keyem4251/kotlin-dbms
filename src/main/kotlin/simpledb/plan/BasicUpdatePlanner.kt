package simpledb.plan

import simpledb.metadata.MetadataManager
import simpledb.parse.*
import simpledb.query.UpdateScan
import simpledb.tx.Transaction

/**
 * 更新系のSQLのためのクエリプランナー
 */
class BasicUpdatePlanner(
    private val metadataManager: MetadataManager,
) : UpdatePlanner {

    /**
     * SelectPlanを呼び条件式を評価、コストを推定し、UpdateScanでテーブルの行を削除していく
     */
    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            updateScan.delete()
            count += 1
        }
        updateScan.close()
        return count
    }

    /**
     * SelectPlanを呼び条件式を評価、コストを推定し、UpdateScanでテーブルの行を更新していく
     */
    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            val value = data.newValue.evaluate(updateScan)
            updateScan.setVal(data.fieldName, value)
            count += 1
        }
        updateScan.close()
        return count
    }

    /**
     * SelectPlanを呼び条件式を評価、コストを推定し、UpdateScanでテーブルの行を挿入していく
     */
    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        val updateScan = plan.open() as UpdateScan
        updateScan.insert()
        val iterator = data.values.iterator()
        for (fieldName in data.fields) {
            val value = iterator.next()
            updateScan.setVal(fieldName, value)
        }
        updateScan.close()
        return 1
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