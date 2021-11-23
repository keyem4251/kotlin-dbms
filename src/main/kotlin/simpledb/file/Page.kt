package simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

val CHARSET = StandardCharsets.US_ASCII

/**
 * Disk blockに保存されている内容（値）を保持するクラス
 *
 * @property charset 文字列に変換するときのコード: ASCII
 * @property bb ByteBuffer バイト型配列のラップして、配列内の値にアクセスする
 *
 * @constructor Buffer Managerから使用される。
 * 使用するBlockサイズを指定してPageを作成する。
 *
 * @constructor Log Managerから使用される。
 * Kotlin(Java)のバイト型配列からPageを作成する。
 */
class Page {
    private var bb: ByteBuffer

    constructor(blockSize: Int) {
        bb = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(b: ByteArray) {
        bb = ByteBuffer.wrap(b)
    }

    /**
     * Page内の[offset]で指定した場所の数値を返す
     * @return Page 内の数値
     */
    fun getInt(offset: Int): Int {
        return bb.getInt(offset)
    }

    /**
     * Page内の[offset]で指定した場所に数値[n]を保存する
     */
    fun setInt(offset: Int, n: Int) {
        bb.putInt(offset, n)
    }

    /**
     * Page内の[offset]で指定した場所のバイナリを返す
     * @return Page 内のバイナリ
     */
    fun getBytes(offset: Int): ByteArray {
        bb.position(offset)
        val length = bb.int
        val b = ByteArray(length)
        bb.get(b)
        return b
    }

    /**
     * Page内の[offset]で指定した場所にバイナリ[b]を保存する
     */
    fun setBytes(offset: Int, b: ByteArray) {
        bb.position(offset)
        bb.putInt(b.size)
        bb.put(b)
    }

    /**
     * Page内の[offset]で指定した場所の文字列を返す
     * @return Page 内の文字列
     */
    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, CHARSET)
    }

    /**
     * Page内の[offset]で指定した場所に文字列[s]を保存する
     */
    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(CHARSET)
        setBytes(offset, b)
    }

    /**
     * 文字列のサイズ[strSize]を受け取りバイナリとしての長さを返す
     * @return 数値
     */
    companion object {
        fun maxLength(strSize: Int): Int {
            val bytesPerChar = CHARSET.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
        }
    }

    /**
     * FileManagerから利用され、Pageの内容を返す
     * @return ByteBuffer
     */
    fun contents(): ByteBuffer {
        bb.position(0)
        return bb
    }
}