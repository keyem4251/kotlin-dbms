package simpledb.metadata

import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

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