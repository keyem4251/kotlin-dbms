package simpledb.parse

class CreateViewData(
    private val viewName: String,
    private val queryData: QueryData,
) {
    fun viewDef(): String {
        return queryData.toString()
    }
}