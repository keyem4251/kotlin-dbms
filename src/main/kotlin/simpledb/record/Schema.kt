package simpledb.record

class Schema {
    private val fields = mutableListOf<String>()
    private val info = mutableMapOf<String, FieldInfo>()

    fun addField(folderName: String, type: Int, length: Int) {
        fields.add(folderName)
        info[folderName] = FieldInfo(type, length)
    }

    fun addIntField(folderName: String) {
        addField(folderName, java.sql.Types.INTEGER, 0)
    }

    fun addStringField(folderName: String, length: Int) {
        addField(folderName, java.sql.Types.VARCHAR, length)
    }

    fun add(folderName: String, schema: Schema) {
        val type = schema.type(folderName) ?: return
        val length = schema.length(folderName) ?: return
        addField(folderName, type, length)
    }

    fun type(folderName: String): Int? {
        return info[folderName]?.type
    }

    fun length(folderName: String): Int? {
        return info[folderName]?.length
    }

    class FieldInfo(val type: Int, val length: Int)
}
