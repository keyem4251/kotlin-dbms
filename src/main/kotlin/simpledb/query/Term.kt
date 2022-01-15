package simpledb.query

class Term(
    private val leftSideExpression: Expression,
    private val rightSideExpression: Expression,
) {
    fun isSatisfied(scan: Scan)
}
