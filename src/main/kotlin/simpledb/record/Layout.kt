package simpledb.record

import simpledb.file.Page
import java.lang.RuntimeException

/**
 * シンプルDBのレコードマネージャ
 * レコードのフィールドとスロットサイズ、スロット内のフィールドの位置など物理情報を持つクラス
 * スロットは1行の長さ（ディスク上で何バイトを1行の保持のためにかかるかの情報）
 *
 * @property schema レコードのフィールドの構成情報
 * @property offsets スロット内のフィールドの位置（フィールド名と位置の辞書）
 * @property slotSize スロットの大きさ
 */
class Layout {
    private var schema: Schema
    private var offsets = mutableMapOf<String, Int>()
    private var slotSize: Int = 0

    /**
     * テーブル作成時に呼ばれる
     * 与えられた[schema]テーブルの構成情報を元に1行の長さを保持する
     */
    constructor(schema: Schema) {
        this.schema = schema
        var pos = Integer.BYTES // space for the empty/inuse flag
        for (fieldName in schema.fields) {
            offsets[fieldName] = pos
            pos += lengthInBytes(fieldName)
        }
        this.slotSize = pos
    }

    /**
     * テーブルがすでに作成されている場合に呼ばれる
     * すでに計算されている（テーブル作成時にわかっている）情報を元にする
     */
    constructor(schema: Schema, offsets: MutableMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }

    /**
     * [fieldName]指定されたフィールド名のスロット内の位置を返す
     * @return スロット内の位置
     */
    fun offset(fieldName: String): Int? {
        return offsets[fieldName]
    }

    /**
     * スロットのサイズを返す
     * @return スロットのサイズ
     */
    fun slotSize(): Int {
        return slotSize
    }

    /**
     * 関連付けられたテーブルの構成情報を返す
     * @return schemaクラス
     */
    fun schema(): Schema {
        return schema
    }

    /**
     * スロットのサイズ、各フィールドのスロット内の位置を決めるために
     * [fieldName]指定されたフィールド名のバイトの長さを返す
     * @return バイトの長さ
     */
    private fun lengthInBytes(fieldName: String): Int {
        val fieldType = schema.type(fieldName)
        if (fieldType == java.sql.Types.INTEGER) return Integer.BYTES
        // fieldType == java.sql.Types.VARCHAR
        val schemaLength = schema.length(fieldName) ?: throw RuntimeException("指定されたフィールドがありません")
        return Page.maxLength(schemaLength)
    }
}
