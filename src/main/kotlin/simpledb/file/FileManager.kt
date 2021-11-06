package simpledb.file

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.lang.RuntimeException

/**
 * OSのファイルシステムを操作するクラス
 * システムが立ち上がる（SimpleDBクラス）際に1つ作成される
 *
 * @property dbDirectory データベースのファイルを保存しているFileクラス
 * @property blockSize 各ブロックのサイズ
 * @property isNew データベースが作成されているかのフラグ
 * @property openFiles 開いているファイル
 */

class FileManager(
    val dbDirectory: File,
    val blockSize: Int,
) {
    var isNew: Boolean = !dbDirectory.exists()
    lateinit var openFiles: MutableMap<String, RandomAccessFile>

    /**
     * 初期化処理
     */
    init {
        // create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs()
        }

        // remove any leftover temporary tables
        val filenames = dbDirectory.list()
        if (filenames != null) {
            for (filename in filenames) {
                if (filename.startsWith("temp")) {
                    File(dbDirectory, filename).delete()
                }
            }
        }
    }

    /**
     * 指定されたブロック[blockId]の内容を指定したページ[page]に読み込む
     */
    @Synchronized
    fun read(blockId: BlockId, page: Page) {
        try {
            val f = getFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            f.channel.read(page.contents())
        } catch (e: IOException) {
            throw RuntimeException("cannot write block $blockId")
        }
    }

    /**
     * 指定されたブロック[blockId]に指定したページ[page]の内容を書き込む
     */
    @Synchronized
    fun write(blockId: BlockId, page: Page) {
        try {
            val f = getFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            f.channel.write(page.contents())
        } catch (e: IOException) {
            throw RuntimeException("cannot write block $blockId")
        }
    }

    /**
     * 指定されたファイル[filename]の末尾に空のバイト配列を書き込み、ファイルを拡張する
     * @return 拡張したブロック
     */
    @Synchronized
    fun append(filename: String): BlockId {
        val newBlockNumber = filename.length
        val blockId = BlockId(filename, newBlockNumber)
        val b = ByteArray(blockSize)
        try {
            val f = getFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            f.write(b)
        } catch (e: IOException) {
            throw RuntimeException("cannot append block $blockId")
        }
        return blockId
    }

    /**
     * 指定したファイル[filename]のブロックの数を返す
     * @return ブロックの数
     */
    fun length(filename: String): Int {
        try {
            val f = getFile(filename)
            return (f.length() / blockSize).toInt()
        } catch (e: IOException) {
            throw RuntimeException("cannot access $filename")
        }
    }

    /**
     * 指定したファイル[filename]を取得する
     * @return RandomAccessFileオブジェクト
     */
    private fun getFile(filename: String): RandomAccessFile {
        var f = openFiles[filename]
        if (f == null) {
            val dbTable = File(dbDirectory, filename)
            f = RandomAccessFile(dbTable, "rws")
            openFiles[filename] = f
        }
        return f
    }
}