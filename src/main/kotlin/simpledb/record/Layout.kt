package simpledb.record

import simpledb.file.Page

class Layout {
    private var schema: Schema
    private var offsets = mutableMapOf<String, Int>()
    private var slotSize: Int = 0

    constructor(schema: Schema) {
        this.schema = schema
        var pos = Integer.BYTES // space for the empty/inuse flag
        for (fieldName in schema.fields) {
            offsets[fieldName] = pos
            pos += lengthInBytes(fieldName)
        }
        this.slotSize = pos
    }

    constructor(schema: Schema, offsets: MutableMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }

    fun offset(fieldName: String): Int? {
        return offsets[fieldName]
    }

    fun slotSize(): Int {
        return slotSize
    }

    fun schema(): Schema {
        return schema
    }

    private fun lengthInBytes(fieldName: String): Int {
        val fieldType = schema.type(fieldName)
        if (fieldType == java.sql.Types.INTEGER) return Integer.BYTES
        val schemaLength = schema.length(fieldName) ?: return 0
        // fieldType == java.sql.Types.VARCHAR
        return Page.maxLength(schemaLength)
    }
}
