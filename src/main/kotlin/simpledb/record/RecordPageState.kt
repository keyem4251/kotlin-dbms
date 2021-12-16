package simpledb.record

enum class RecordPageState(val id: Int) {
    EMPTY(0),
    USED(1),
}
