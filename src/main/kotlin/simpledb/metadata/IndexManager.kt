package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

class IndexManager(
    private val isNew: Boolean,
    private val tableManager: TableManager,
    private val statisticsManager: StatisticsManager,
    private val transaction: Transaction,
) {
    private var layout: Layout

    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField("indexname", MAX_NAME)
            schema.addStringField("tablename", MAX_NAME)
            schema.addStringField("fieldname", MAX_NAME)
            tableManager.createTable("indexcatalog", schema, transaction)
        }
        layout = tableManager.getLayout("indexcatalog", transaction)
    }

    /**
     * [indexName]指定されたインデックス名、[tableName]テーブル名、[fieldName]フィールド名で
     * indexcatalog（インデックス名、テーブル名、フィールド名をテーブルに記録する）にデータを挿入する
     */
    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        val tableScan = TableScan(tx, "indexcatalog", layout)
        tableScan.insert()
        tableScan.setString("indexname", indexName)
        tableScan.setString("tablename", tableName)
        tableScan.setString("fieldname", fieldName)
        tableScan.close()
    }

    /**
     * [tableName]指定されたテーブルに設定されているインデックスをマップで返す
     * @return インデックスのマップ<フィールド名、インデックスの情報>
     */
    fun getIndexInfo(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        val result = mutableMapOf<String, IndexInfo>()
        val tableScan = TableScan(tx, "indexcatalog", layout)
        while (tableScan.next()) {
            if (tableScan.getString("tablename") == tableName) {
                val indexName = tableScan.getString("indexname")
                val fieldName = tableScan.getString("fieldname")
                val tableLayout = tableManager.getLayout(tableName, tx)
                val tableStatisticsInformation = statisticsManager.getStatisticsInformation(tableName, tableLayout, tx)
                val indexInfo = IndexInfo(indexName, fieldName, tx, tableLayout.schema(), tableStatisticsInformation)
                result[fieldName] = indexInfo
            }
        }
        tableScan.close()
        return result
    }
}