package simpledb

import simpledb.query.Constant
import simpledb.record.RID

interface Index {

    fun beforeFirst(searchKey: Constant)

    fun next(): Boolean

    fun getDataRid(): RID

    fun insert(dataValue: Constant, dataRid: RID)

    fun delete(dataValue: Constant, dataRid: RID)

    fun close()
}