package simpledb.index.btree

import simpledb.query.Constant

data class DirEntry(
    val dataVal: Constant,
    val blockNumber: Int,
)