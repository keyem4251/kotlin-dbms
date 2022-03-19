package simpledb.jdbc

import java.sql.ResultSetMetaData
import java.sql.SQLException

abstract class ResultSetMetaDataAdapter: ResultSetMetaData {
    override fun getCatalogName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getColumnClassName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getColumnCount(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getColumnDisplaySize(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun getColumnLabel(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getColumnName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getColumnType(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun getColumnTypeName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getPrecision(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun getScale(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun getSchemaName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun getTableName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    override fun isAutoIncrement(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isCaseSensitive(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isCurrency(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isDefinitelyWritable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isNullable(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun isReadOnly(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        throw SQLException("operation not implemented")
    }

    override fun isSearchable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isSigned(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isWritable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }
}