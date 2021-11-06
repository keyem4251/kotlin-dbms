package simpledb.file

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.lang.RuntimeException

class FileManager(
    val dbDirectory: File,
    val blockSize: Int,
) {
    var isNew: Boolean = !dbDirectory.exists()
    lateinit var openFiles: MutableMap<String, RandomAccessFile>

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

    fun length(filename: String): Int {
        try {
            val f = getFile(filename)
            return (f.length() / blockSize).toInt()
        } catch (e: IOException) {
            throw RuntimeException("cannot access $filename")
        }
    }

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