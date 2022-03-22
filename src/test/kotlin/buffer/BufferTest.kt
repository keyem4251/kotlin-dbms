package buffer

import org.junit.Test
import simpledb.file.BlockId
import simpledb.server.SimpleDB

class BufferTest {
    @Test
    fun testBuffer() {
        val db = SimpleDB("buffertest", 400, 3)
        val bufferManager = db.bufferManager

        val buffer1 = bufferManager.pin(BlockId("testfile", 1))
        val page = buffer1.contents()
        val n = page.getInt(80)
        page.setInt(80, n+1)
        buffer1.setModified(1, 0) // place holder values
        println("The new value is ${n+1}")
        bufferManager.unpin(buffer1)

        // One of these pins will flush buffer1 to disk
        var buffer2 = bufferManager.pin(BlockId("testfile", 2))
        val buffer3 = bufferManager.pin(BlockId("testfile", 3))
        val buffer4 = bufferManager.pin(BlockId("testfile", 4))

        bufferManager.unpin(buffer2)
        buffer2 = bufferManager.pin(BlockId("testfile", 1))
        val page2 = buffer2.contents()
        page2.setInt(80, 9999) // This modification won't get written to disk
        buffer2.setModified(1, 0)
    }
}