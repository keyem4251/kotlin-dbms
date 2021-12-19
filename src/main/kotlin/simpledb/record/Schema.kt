package simpledb.record

/**
 * シンプルDBのレコードマネージャ
 * レコードの構成情報を持つ（フィールドの名前、フィールドの型、フィールドの長さ（文字列のみ））
 * ユーザーがテーブルを作成する際に指定する情報でバイトの長さ、ディスクの位置など物理情報は含まない
 *
 * @property fields フィールド名のリスト
 * @property info フィールド名とフィールド情報（型、長さ）の辞書
 */
class Schema {
    val fields = mutableListOf<String>()
    private val info = mutableMapOf<String, FieldInfo>()

    /**
     * 指定された[fieldName]フィールド名、[type]型、[length]長さのフィールドを追加する
     */
    fun addField(fieldName: String, type: Int, length: Int) {
        fields.add(fieldName)
        info[fieldName] = FieldInfo(type, length)
    }

    /**
     * 指定された[fieldName]フィールド名の数値フィールドを追加する
     * 数値なので長さは0（長さは文字列のみ）
     */
    fun addIntField(fieldName: String) {
        addField(fieldName, java.sql.Types.INTEGER, 0)
    }

    /**
     * 指定された[fieldName]フィールド名、[length]長さの文字列フィールドを追加する
     */
    fun addStringField(fieldName: String, length: Int) {
        addField(fieldName, java.sql.Types.VARCHAR, length)
    }

    /**
     * [schema]他のスキーマから[fieldName]指定されたフィールドをコピーする
     */
    fun add(fieldName: String, schema: Schema) {
        val type = schema.type(fieldName) ?: return
        val length = schema.length(fieldName) ?: return
        addField(fieldName, type, length)
    }

    /**
     * [schema]他のスキーマからすべてのフィールドをコピーする
     */
    fun addAll(schema: Schema) {
        for (fieldName in schema.fields) {
            add(fieldName, schema)
        }
    }

    /**
     * [fieldName]指定されたフィールドを保持するかの判定
     * @return 保持する場合はtrue
     */
    fun hasField(fieldName: String): Boolean {
        return fields.contains(fieldName)
    }

    /**
     * [fieldName]指定されたフィールドの型を返す
     * @return フィールドの型
     */
    fun type(fieldName: String): Int? {
        return info[fieldName]?.type
    }

    /**
     * [fieldName]指定されたフィールドの長さを返す
     * @return フィールドの長さ（数値は0、文字列は追加時に指定した長さ）
     */
    fun length(fieldName: String): Int? {
        return info[fieldName]?.length
    }

    /**
     * フィールドの型、長さを保持するクラス
     */
    class FieldInfo(val type: Int, val length: Int)
}
