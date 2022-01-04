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
 * 
 * システムが立ち上がり、データベースが作成されたときにクラスが作成される
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

    /**
     * [viewName]指定されたビューの名前、[viewDef]指定されたビューの定義を元にカタログテーブルにレコードを挿入する
     * カタログテーブルにアクセスするためTableScanクラスを使用する
     */
    fun createView(viewName: String, viewDef: String, transaction: Transaction) {
        val layout = tableManager.getLayout("viewcatalog", transaction)
        val tableScan = TableScan(transaction, "viewcatalog", layout)
        tableScan.setString("viewname", viewName)
        tableScan.setString("viewdef", viewDef)
        tableScan.close()
    }

    /**
     * [viewName]指定されたビューの名前のビューの定義を返す
     * カタログテーブルにアクセスするためTableScanクラスを使用する
     * @return ビュー定義、該当する定義がない場合はnullを返す
     */
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