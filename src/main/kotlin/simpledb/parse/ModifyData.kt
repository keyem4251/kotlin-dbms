package simpledb.parse

import simpledb.query.Expression
import simpledb.query.Predicate

/**
 * Updateを行うのに必要な値を格納するクラス
 * <Modify> := UPDATE IdToken SET <Field> = <Expression> [ WHERE <Predicate> ]
 */
class ModifyData(
    private val tableName: String,
    private val fieldName: String,
    private val newValue: Expression,
    private val predicate: Predicate,
)