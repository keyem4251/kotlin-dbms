package metadata

import org.junit.Test
import simpledb.metadata.TableManager
import simpledb.record.Schema
import simpledb.server.SimpleDB

class TableManagerTest {
    @Test
    fun testTableManager() {
        val db = SimpleDB("testdata/tablemanagertest", 400, 8)
        val transaction = db.newTransaction()
        val tableManager = TableManager(true, transaction)

        val schema = Schema()
        schema.addIntField("A")
        schema.addStringField("B", 9)
        tableManager.createTable("MyTable", schema, transaction)

        val layout = tableManager.getLayout("MyTable", transaction)
        val size = layout.slotSize()
        val schema2 = layout.schema()
        println("MyTable has slot size $size")
        println("Its fields are:")
        for (fieldName in schema2.fields) {
            val type = if (schema2.type(fieldName) == java.sql.Types.INTEGER) {
                "int"
            } else {
                val stringLen = schema2.length(fieldName)
                "varchar($stringLen)"
            }
            println("$fieldName: $type")
        }
        transaction.commit()
    }
}