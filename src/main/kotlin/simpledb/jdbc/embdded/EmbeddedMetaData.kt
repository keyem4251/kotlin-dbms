package simpledb.jdbc.embdded

import simpledb.jdbc.ResultSetMetaDataAdapter
import simpledb.record.Schema

class EmbeddedMetaData(
    private val schema: Schema,
) : ResultSetMetaDataAdapter() {
    override fun getColumnCount(): Int {
        return schema.fields.size
    }

    override fun getColumnName(column: Int): String {
        return schema.fields[column-1]
    }

    override fun getColumnType(column: Int): Int {
        val fieldName = getColumnName(column)
        return schema.type(fieldName) ?: throw RuntimeException("null error")
    }

    override fun getColumnDisplaySize(column: Int): Int {
        val fieldName = getColumnName(column)
        val fieldType = schema.type(fieldName) ?: throw RuntimeException("null error")
        val fieldLength = if (fieldType == java.sql.Types.INTEGER) {
            6
        } else {
            schema.length(fieldName)
        } ?: throw RuntimeException("null error")
        return fieldName.length.coerceAtLeast(fieldLength) + 1
    }
}