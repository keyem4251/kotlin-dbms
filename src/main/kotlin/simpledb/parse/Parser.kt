package simpledb.parse

import simpledb.query.Constant
import simpledb.query.Expression
import simpledb.query.Predicate
import simpledb.query.Term
import simpledb.record.Schema
import java.util.*

/**
 * 条件式と値を扱う5種類のメソッドに値を返すようにしたクラス
 * field, constant, expression, term, predicate以外のSQLの解析も行う（create, select, delete..など）
 *
 */
class Parser(private val string: String) {
    private val lexer = Lexer(string)

    // Methods for parsing predicates and their components
    fun field(): String {
        return lexer.eatId()
    }

    fun constant(): Constant {
        return if (lexer.matchStringConstant()) {
            Constant(lexer.eatStringConstant())
        } else {
            Constant(lexer.eatIntConstant())
        }
    }

    fun expression(): Expression {
        return if (lexer.matchId()) {
            Expression(field())
        } else {
            Expression(constant())
        }
    }

    fun term(): Term {
        val leftSideExpression = expression()
        lexer.eatDelimiter('=')
        val rightSideExpression = expression()
        return Term(leftSideExpression, rightSideExpression)
    }

    fun predicate(): Predicate {
        val predicate = Predicate(term())
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate.conjoinWith(predicate())
        }
        return predicate
    }

    // Methods for parsing queries
    fun query(): QueryData {
        lexer.eatKeyword("select")
        val fields = selectList()
        lexer.eatKeyword("from")
        val tables = tableList()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        return QueryData(fields, tables, predicate)
    }

    private fun selectList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(field())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(selectList())
        }
        return mutableList
    }

    private fun tableList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(lexer.eatId())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(tableList())
        }
        return mutableList
    }

    // Methods for parsing the various update commands
    fun updateCmd(): Any {
        return if (lexer.matchKeyword("insert")) {
            insert()
        } else if (lexer.matchKeyword("delete")) {
            delete()
        } else if (lexer.matchKeyword("update")) {
            modify()
        } else {
            create()
        }
    }

    private fun create(): Any {
        lexer.eatKeyword("create")
        return if (lexer.matchKeyword("table")) {
            createTable()
        } else if (lexer.matchKeyword("view")) {
            createView()
        } else {
            createIndex()
        }
    }

    // Methods for parsing delete commands
    fun delete(): DeleteData {
        lexer.eatKeyword("delete")
        lexer.eatKeyword("from")
        val tableName = lexer.eatId()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        return DeleteData(tableName, predicate)
    }

    // Methods for parsing insert commands
    fun insert(): InsertData {
        lexer.eatKeyword("insert")
        lexer.eatKeyword("into")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val fields = fieldList()
        lexer.eatDelimiter(')')
        lexer.eatKeyword("values")
        lexer.eatDelimiter('(')
        val values = constList()
        lexer.eatDelimiter(')')
        return InsertData(tableName, fields, values)
    }

    private fun fieldList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(field())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(fieldList())
        }
        return mutableList
    }

    private fun constList(): MutableList<Constant> {
        val mutableList = mutableListOf<Constant>()
        mutableList.add(constant())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(constList())
        }
        return mutableList
    }

    // Method for parsing modify commands
    fun modify(): ModifyData {
        lexer.eatKeyword("update")
        val tableName = lexer.eatId()
        lexer.eatKeyword("set")
        val fieldName = field()
        lexer.eatDelimiter('=')
        val newValue = expression()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        return ModifyData(tableName, fieldName, newValue, predicate)
    }

    // Method for parsing create table commands
    fun createTable(): CreateTableData {
        lexer.eatKeyword("table")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val schema = fieldDefs()
        lexer.eatDelimiter(')')
        return CreateTableData(tableName, schema)
    }

    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            val schema2 = fieldDefs()
            schema.addAll(schema2)
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    private fun fieldType(fieldName: String): Schema {
        val schema = Schema()
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int")
            schema.addIntField(fieldName)
        } else {
            lexer.eatKeyword("varchar")
            lexer.eatDelimiter('(')
            val stringLength = lexer.eatIntConstant()
            lexer.eatDelimiter(')')
            schema.addStringField(fieldName, stringLength)
        }
        return schema
    }

    fun createView(): CreateViewData {
        lexer.eatKeyword("view")
        val viewName = lexer.eatId()
        lexer.eatKeyword("as")
        val queryData = query()
        return CreateViewData(viewName, queryData)
    }

    fun createIndex(): CreateIndexData {
        lexer.eatKeyword("index")
        val indexName = lexer.eatId()
        lexer.eatKeyword("on")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val fieldName = field()
        lexer.eatDelimiter(')')
        return CreateIndexData(indexName, tableName, fieldName)
    }
}