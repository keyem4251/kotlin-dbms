package simpledb.parse

import simpledb.query.Constant
import simpledb.query.Expression
import simpledb.query.Predicate
import simpledb.query.Term
import simpledb.record.Schema

/**
 * 条件式と値を扱う5種類のメソッドに値を返すようにしたクラス
 * field, constant, expression, term, predicate以外のSQLの解析も行う（create, select, delete..など）
 * <Field>          := IdToken
 * <Constant>       := StringToken | IntToken
 * <Expression>     := <Field> | <Constant>
 * <Term>           := <Expression> = <Expression>
 * <Predicate>      := <Term> [ AND <Predicate> ]
 *
 * <Query>          := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
 * <SelectList>     := <Field> [ , <SelectList> ]
 * <TableList>      := IdToken [ , <TableList> ]
 *
 * <UpdateCmd>      := <Insert> | <Delete> | <Modify> | <Create>
 * <Create>         := <CreateTable> | <CreateView> | <CreateIndex>
 *
 * <Insert>         := INSERT INTO IdToken ( <FieldList> ) VALUES ( <ConstList> )
 * <FieldList>      := <Field> [ , <FieldList> ]
 * <ConstList>      := <Constant> [ , <ConstList> ]
 *
 * <Delete>         := DELETE FROM IdToken [ WHERE <Predicate> ]
 *
 * <Modify>         := UPDATE IdToken SET <Field> = <Expression> [ WHERE <Predicate> ]
 *
 * <CreateTable>    := CREATE TABLE IdToken ( <FieldDefs> )
 * <FieldDefs>      := <FieldDef> [ , <FieldDefs> ]
 * <FieldDef>       := IdToken <TypeDef>
 * <TypeDef>        := INT | VARCHAR ( IntToken )
 *
 * <CreateView>     := CREATE VIEW IdToken AS <Query>
 *
 * <CreateIndex>    := CREATE INDEX IdToken ON IdToken ( <Field> )
 */
class Parser(private val string: String) {
    private val lexer = Lexer(string)

    // Methods for parsing predicates and their components
    /**
     * フィールド名を解析し値を返す
     * @return フィールド名
     */
    fun field(): String {
        return lexer.eatId()
    }

    /**
     * フィールドの値を解析し値を返す
     * 検索した条件式の中の行の値
     * @return 文字列、数値
     */
    fun constant(): Constant {
        return if (lexer.matchStringConstant()) {
            Constant(lexer.eatStringConstant())
        } else {
            Constant(lexer.eatIntConstant())
        }
    }

    /**
     * テーブルの列と値を解析し値を返す
     * 「ID = 1」の「ID」という列名なのか、「1」という値なのか
     * @return 列名、値のExpressionを返す
     */
    fun expression(): Expression {
        return if (lexer.matchId()) {
            Expression(field())
        } else {
            Expression(constant())
        }
    }

    /**
     * 条件式の中のそれぞれの項を解析し値を返す
     * 「ID = 1」の「=」を除いた左右の値（ID、1）
     * @return 列名と値をTermクラスとして返す
     */
    fun term(): Term {
        val leftSideExpression = expression()
        lexer.eatDelimiter('=')
        val rightSideExpression = expression()
        return Term(leftSideExpression, rightSideExpression)
    }

    /**
     * 条件式を解析し返す
     * @return 条件式をPredicateクラスとして返す
     */
    fun predicate(): Predicate {
        val predicate = Predicate(term())
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate.conjoinWith(predicate())
        }
        return predicate
    }

    // Methods for parsing queries
    /**
     * SQLクエリを解析しQueryDataとして値を返す
     * @return QueryDataクラス
     */
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

    /**
     * selectするフィールド名のリストを返す
     * @return フィールド名のリスト
     */
    private fun selectList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(field())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(selectList())
        }
        return mutableList
    }

    /**
     * SQLを実行するテーブル名のリストを返す
     * @return テーブル名のリストを返す
     */
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
    /**
     * テーブルに対する更新処理を解析し、更新に必要な値を格納した該当するDataクラスを返す
     * @return InsertData, DeleteData, UpdateData, CreateDataのいずれか
     */
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

    /**
     * createのSQLを解析し、table、view、indexのどれかを解析し作成に必要な値を格納したクラスを返す
     * @return CreateTableData, CreateViewData, CreateIndexData
     */
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
    /**
     * DeleteのSQLを解析しDeleteに必要な値をクラスに格納して返す
     * @return DeleteDataクラス
     */
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
    /**
     * InsertのSQLを解析しInsertに必要な値をクラスに格納して返す
     */
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

    /**
     * insertを行うフィールド名のリストを返す
     * @return フィールド名のリスト
     */
    private fun fieldList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(field())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(fieldList())
        }
        return mutableList
    }

    /**
     * insertを行う値のリストを返す
     * @return 値のリスト
     */
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
    /**
     * UpdateのSQLを解析し、値の更新に必要な値を格納したクラスを返す
     */
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
    /**
     * テーブルをCreateするSQLを解析し、作成に必要な値を格納したクラスを返す
     */
    fun createTable(): CreateTableData {
        lexer.eatKeyword("table")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val schema = fieldDefs()
        lexer.eatDelimiter(')')
        return CreateTableData(tableName, schema)
    }

    /**
     * SQLから列名などテーブルの構造を再帰的に解析する
     */
    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            val schema2 = fieldDefs()
            schema.addAll(schema2)
        }
        return schema
    }

    /**
     * フィールド名などテーブルの構造を解析する
     */
    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    /**
     * 指定されたフィールド[fieldName]のフィールドの型を返す
     */
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

    /**
     * ビューをCreateするSQLを解析し、作成に必要な値を格納したクラスを返す
     */
    fun createView(): CreateViewData {
        lexer.eatKeyword("view")
        val viewName = lexer.eatId()
        lexer.eatKeyword("as")
        val queryData = query()
        return CreateViewData(viewName, queryData)
    }

    /**
     * インデックスをCreateするSQLを解析し、作成に必要な値を格納したクラスを返す
     */
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