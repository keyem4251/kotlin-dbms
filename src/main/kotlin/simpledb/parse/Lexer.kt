package simpledb.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

/**
 * 文字列の並びを解析しSQLの内容をトークンに分解し、値を返していく
 * eat系: 現在のトークンを解析し、値を返すメソッド
 * match系: 現在のトークンが期待する値かを判定するメソッド
 *
 * @property tokenizer StreamTokenizerを使い構文解析を行い、入力を1つづつのトークンに分ける
 * @property keywords SQLに使用される単語
 */
class Lexer(string: String) {
    private lateinit var keywords: MutableCollection<String>
    private var tokenizer: StreamTokenizer

    init {
        initKeywords()
        tokenizer = StreamTokenizer(StringReader(string))
        tokenizer.ordinaryChar(".".toInt())
        tokenizer.wordChars("_".toInt(), "_".toInt())
        tokenizer.lowerCaseMode(true)
        nextToken()
    }

    // 現在の単語が期待するものか判定する
    /**
     * 区切り記号かを判定する
     * @return 区切り記号ならtrue、そうでなければfalse
     */
    fun matchDelimiter(delimiter: Char): Boolean {
        return delimiter == tokenizer.ttype as Char
    }

    /**
     * 数値かを判定する
     * @return 数値ならtrue、そうでなければfalse
     */
    fun matchIntConstant(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER
    }

    /**
     * 文字列かを判定する
     * @return 文字列ならtrue、そうでなければfalse
     */
    fun matchStringConstant(): Boolean {
        return '\'' == tokenizer.ttype as Char
    }

    /**
     * SQLの慣用句かを判定する
     * @return 慣用句かならtrue、そうでなければfalse
     */
    fun matchKeyword(word: String): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval.equals(word)
    }

    /**
     * 文字列かつ、SQLの慣用句かを判定する
     * @return 文字列かつSQLの慣用句でなければtrue、そうでなければfalse
     */
    fun matchId(): Boolean{
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval)
    }

    // 現在の単語が期待するものか判定し値を返す
    /**
     * 区切り文字を解析し字句解析をすすめる
     */
    fun eatDelimiter(delimiter: Char) {
        if (!matchDelimiter(delimiter)) throw BadSyntaxException()
        nextToken()
    }

    /**
     * 数値のConstant型かを判定しそうであれば、次の単語に字句解析を勧め、現在の単語を返す
     * @return 現在の単語: 数値の型
     */
    fun eatIntConstant(): Int {
        if (!matchIntConstant()) throw BadSyntaxException()
        val i = tokenizer.nval as Int
        nextToken()
        return i
    }

    /**
     * StringのConstant型かを判定しそうであれば、次の単語に字句解析を勧め、現在の単語を返す
     * @return 現在の単語: String型
     */
    fun eatStringConstant(): String {
        if (!matchStringConstant()) throw BadSyntaxException()
        val s = tokenizer.sval
        nextToken()
        return s
    }

    /**
     * SQLの慣用句を解析し字句解析をすすめる
     */
    fun eatKeyword(word: String) {
        if (!matchKeyword(word)) throw BadSyntaxException()
        nextToken()
    }

    /**
     * String型かを判定しそうであれば、次の単語に字句解析を勧め、現在の単語を返す
     * @return 現在の単語: String
     */
    fun eatId(): String {
        if (!matchId()) throw BadSyntaxException()
        val s = tokenizer.sval
        nextToken()
        return s
    }

    /**
     * 次の単語に処理をすすめる
     */
    private fun nextToken() {
        try {
            tokenizer.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException()
        }
    }

    /**
     * SQLでサポートする慣用句
     */
    private fun initKeywords() {
        keywords = mutableListOf(
            "select", "from", "where", "and", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "varchar", "int", "view", "as", "index", "on"
        )
    }
}