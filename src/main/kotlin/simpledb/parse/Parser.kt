package simpledb.parse

import simpledb.query.Constant
import simpledb.query.Expression
import simpledb.query.Predicate
import simpledb.query.Term
import java.util.*

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
//    fun updateCmd(): Objects {
//
//    }

//    private fun create(): Objects {
//        lexer.eatKeyword("create")
//        if (lexer.matchKeyword("table")) {
//            return create
//        }
//    }

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
}