package simpledb.metadata

import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

/**
 * ビューはクエリから動的に計算されるレコードを持つテーブル
 * メタデータマネージャは各ビューの定義を保存し、要求された際にビューを定義しているクエリを返す
 * ビューマネージャーはカタログテーブル（view catalog）にビューの定義を保存します
 * 形式は1つのビューごとに1つのレコード
 * 1レコードはviewcat(ViewName, ViewDef)よりViewName, ViewDefの2つのフィールドを持つ
 */
class ViewManager(
    private val isNew: Boolean,
    val tableManager: TableManager,
    private val transaction: Transaction,
) {
    private val max_viewdef = 100

    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField("viewname", MAX_NAME)
            schema.addStringField("viewdef", max_viewdef)
            tableManager.createTable("viewcatalog", schema, transaction)
        }
    }

    fun createView(viewName: String, viewDef: String, transaction: Transaction) {
        val layout = tableManager.getLayout("viewcatalog", transaction)
        val tableScan = TableScan(transaction, "viewcatalog", layout)
        tableScan.setString("viewname", viewName)
        tableScan.setString("viewdef", viewDef)
        tableScan.close()
    }

    fun getViewDef(viewName: String, transaction: Transaction): String? {
        var result: String? = null
        val layout = tableManager.getLayout("viewcatalog", transaction)
        val tableScan = TableScan(transaction, "viewcatalog", layout)
        while (tableScan.next()) {
            if (tableScan.getString("viewname") == viewName) {
                result = tableScan.getString("viewdef")
                break
            }
        }
        tableScan.close()
        return result
    }
}