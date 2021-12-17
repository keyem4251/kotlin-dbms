package simpledb.record

class Schema {
    val fields = mutableListOf<String>()
    private val info = mutableMapOf<String, FieldInfo>()

    fun addField(fieldName: String, type: Int, length: Int) {
        fields.add(fieldName)
        info[fieldName] = FieldInfo(type, length)
    }

    fun addIntField(fieldName: String) {
        addField(fieldName, java.sql.Types.INTEGER, 0)
    }

    fun addStringField(fieldName: String, length: Int) {
        addField(fieldName, java.sql.Types.VARCHAR, length)
    }

    fun add(fieldName: String, schema: Schema) {
        val type = schema.type(fieldName) ?: return
        val length = schema.length(fieldName) ?: return
        addField(fieldName, type, length)
    }

    fun addAll(schema: Schema) {
        for (fieldName in schema.fields) {
            add(fieldName, schema)
        }
    }

    fun hasField(fieldName: String): Boolean {
        return fields.contains(fieldName)
    }

    fun type(fieldName: String): Int? {
        return info[fieldName]?.type
    }

    fun length(fieldName: String): Int? {
        return info[fieldName]?.length
    }

    class FieldInfo(val type: Int, val length: Int)
}
