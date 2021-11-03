package simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

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
    val charset: Charset = StandardCharsets.US_ASCII
    private var bb: ByteBuffer

    constructor(blockSize: Int) {
        bb = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(b: ByteArray) {
        bb = ByteBuffer.wrap(b)
    }

    /**
     * get an Integer from [offset] int the page by calling the ByteBuffer
     * @return Integer value in the page
     */
    fun getInt(offset: Int): Int {
        return bb.getInt(offset)
    }

    /**
     * save an Int [n] to [offset] in the page by calling the ByteBuffer
     */
    fun setInt(offset: Int, n: Int) {
        bb.putInt(offset, n)
    }

    fun getBytes(offset: Int): ByteArray {
        bb.position(offset)
        val length = bb.int
        val b = ByteArray(length)
        bb.get(b)
        return b
    }

    fun setBytes(offset: Int, b: ByteArray) {
        bb.position(offset)
        bb.putInt(b.size)
        bb.put(b)
    }

    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, charset)
    }

    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(charset)
        setBytes(offset, b)
    }

    fun maxLength(strSize: Int): Int {
        val bytesPerChar = charset.newEncoder().maxBytesPerChar()
        return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
    }

    fun contents(): ByteBuffer {
        bb.position(0)
        return bb
    }
}