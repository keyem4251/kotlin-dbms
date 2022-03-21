package simpledb.server

import simpledb.jdbc.embdded.EmbeddedDriver
import java.sql.*
import java.util.*

fun main(args: Array<String>) {
    val scanner = Scanner(System.`in`)
    println("Connect> ")
    val connectionString = scanner.nextLine()
    val driver = EmbeddedDriver()
    try {
        val connection: Connection = driver.connect(connectionString, null)
        val statement: Statement = connection.createStatement()
        print("\nSQL> ")
        while (scanner.hasNextLine()) {
            // process one line of input
            val cmd = scanner.nextLine().trim()
            if (cmd.startsWith("exit")) {
                break
            } else if (cmd.startsWith("select")) {
                // do select query
                doQuery(statement, cmd)
            } else {
                // do update query
            }
            print("\nSQL> ")
        }
    } catch (e: SQLException) {
        e.printStackTrace()
    }
    scanner.close()
}

private fun doQuery(statement: Statement, cmd: String) {
    try {
        val resultSet: ResultSet = statement.executeQuery(cmd)
        val metaData: ResultSetMetaData = resultSet.metaData
        val columnCount = metaData.columnCount
        var totalwidth = 0

        // print header
        for (i in 1..columnCount) {
            val fieldName = metaData.getCatalogName(i)
            val width = metaData.getColumnDisplaySize(i)
            totalwidth += width
            val fmt = "%${width}s"
            print("${fmt}$fieldName")
        }
        println()
        for (i in 1..totalwidth) {
            print("-")
        }
        println()

        // print records
        for (i in 1..columnCount) {
            val fieldName = metaData.getCatalogName(i)
            val fieldType = metaData.getColumnType(i)
            val fmt = "%${metaData.getColumnDisplaySize(i)}"
            if (fieldType == java.sql.Types.INTEGER) {
                val intValue = resultSet.getInt(fieldName)
                print("$fmt$intValue")
            } else {
                val intString = resultSet.getString(fieldName)
                print("$fmt$intString")
            }
        }
        println()
    } catch (e: SQLException) {
        println("SQL Exception: ${e.message}")
    }
}

private fun doUpdate(statement: Statement, cmd: String) {
    try {
        val howMany = statement.executeUpdate(cmd)
        println("$howMany records processed")
    } catch (e: SQLException) {
        println("SQL Exception: ${e.message}")
    }
}
