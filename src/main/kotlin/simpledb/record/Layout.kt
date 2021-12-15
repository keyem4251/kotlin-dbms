package simpledb.record

import simpledb.file.Page

class Layout {
    private var schema: Schema
    private var offsets = mutableMapOf<String, Int>()
    private var slotSize: Int = 0

    constructor(schema: Schema) {
        this.schema = schema
        var pos = Integer.BYTES // space for the empty/inuse flag
        for (folderName in schema.fields) {
            offsets[folderName] = pos
            pos += lengthInBytes(folderName)
        }
        this.slotSize = pos
    }

    constructor(schema: Schema, offsets: MutableMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }

    fun offset(folderName: String): Int? {
        return offsets[folderName]
    }

    fun slotSize(): Int {
        return slotSize
    }

    private fun lengthInBytes(folderName: String): Int {
        val folderType = schema.type(folderName)
        if (folderType == java.sql.Types.INTEGER) return Integer.BYTES
        val schemaLength = schema.length(folderName) ?: return 0
        // folderType == java.sql.Types.VARCHAR
        return Page.maxLength(schemaLength)
    }
}
