package simpledb.jdbc.embdded

import simpledb.jdbc.ResultSetAdapter
import simpledb.plan.Plan
import simpledb.query.Scan
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.*

class EmbeddedResultSet(
    private val plan: Plan,
    private val connection: EmbeddedConnection,
) : ResultSetAdapter() {
    private val scan: Scan = plan.open()
    private val schema = plan.schema()

    override fun next(): Boolean {
        try {
            return scan.next()
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getInt(columnLabel: String?): Int {
        try {
            val fieldName = columnLabel?.lowercase(Locale.getDefault()) ?: throw RuntimeException("null error")
            return scan.getInt(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getString(columnLabel: String?): String {
        try {
            val fieldName = columnLabel?.lowercase(Locale.getDefault()) ?: throw RuntimeException("null error")
            return scan.getString(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getMetaData(): ResultSetMetaData {
        return EmbeddedMetaData(schema)
    }

    override fun close() {
        scan.close()
        connection.commit()
    }
}