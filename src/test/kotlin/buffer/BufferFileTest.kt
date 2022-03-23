package buffer

import org.junit.Test
import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.server.SimpleDB

class BufferFileTest {
    @Test
    fun testBufferFile() {
        val db = SimpleDB("testdata/bufferfiletest", 400, 8)
        val bufferManager = db.bufferManager
        val blockId = BlockId("testfile", 2)
        val pos1 = 80

        val buffer1 = bufferManager.pin(blockId)
        val page1 = buffer1.contents()
        page1.setString(pos1, "abcdefghijklm")
        val size = Page.maxLength("abcdefghijklm".length)
        val pos2 = pos1 + size
        page1.setInt(pos2, 345)
        buffer1.setModified(1, 0)
        bufferManager.unpin(buffer1)

        val buffer2 = bufferManager.pin(blockId)
        val page2 = buffer2.contents()
        println("offset $pos2 contains ${page2.getInt(pos2)}")
        println("offset $pos1 contains ${page2.getString(pos1)}")
        bufferManager.unpin(buffer2)
    }
}