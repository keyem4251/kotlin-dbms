package simpledb.parse

import simpledb.query.Expression
import simpledb.query.Predicate

/**
 * Updateを行うのに必要な値を格納するクラス
 * <Modify> := UPDATE IdToken SET <Field> = <Expression> [ WHERE <Predicate> ]
 */
class ModifyData(
    val tableName: String,
    val fieldName: String,
    val newValue: Expression,
    val predicate: Predicate,
)