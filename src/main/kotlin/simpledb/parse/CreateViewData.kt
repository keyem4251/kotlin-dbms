package simpledb.parse

/**
 * Viewを作成するのに必要な値を格納するクラス
 * <CreateView> := CREATE VIEW IdToken AS <Query>
 */
class CreateViewData(
    private val viewName: String,
    private val queryData: QueryData,
) {
    fun viewDef(): String {
        return queryData.toString()
    }
}