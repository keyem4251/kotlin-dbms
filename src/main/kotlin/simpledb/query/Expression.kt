package simpledb.query

class Expression {
    private var value: Constant? = null
    private var fieldName: String? = ""

    constructor(value: Constant) {
        this.value = value
    }

    constructor(fieldName: String) {
        this.fieldName = fieldName
    }

    fun evaluate(scan: Scan): Constant {
        return if (value != null) {
            value!!
        } else if (fieldName != null) {
            scan.getVal(fieldName!!)
        } else {
            throw RuntimeException("null error")
        }
    }
}
