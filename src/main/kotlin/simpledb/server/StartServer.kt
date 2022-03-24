package simpledb.server

import simpledb.jdbc.embdded.EmbeddedDriver
import java.sql.*
import java.util.*

fun main(args: Array<String>) {
    val scanner = Scanner(System.`in`)
    print("Connect> ")
    val connectionString = scanner.nextLine()
    val driver = EmbeddedDriver()
    try {
        val connection = driver.connect(connectionString.replace(":", "/"), null)
        val statement = connection.createStatement()
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
                doUpdate(statement, cmd)
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
        val resultSet = statement.executeQuery(cmd)
        val metaData = resultSet.metaData
        val columnCount = metaData.columnCount
        var totalwidth = 0
        // print header
        for (i in 1..columnCount) {
            val fieldName = metaData.getColumnName(i)
            val width = metaData.getColumnDisplaySize(i)
            totalwidth += (width + fieldName.length)
            val space = StringBuilder()
            for (j in 1..width) {
                space.append(" ")
            }
            print("$space$fieldName")
        }
        println()
        for (i in 1..totalwidth) {
            print("-")
        }
        println()

        // print records
        while (resultSet.next()) {
            for (i in 1..columnCount) {
                val fieldName = metaData.getColumnName(i)
                val fieldType = metaData.getColumnType(i)
                val width = metaData.getColumnDisplaySize(i)
                val space = StringBuilder()
                if (fieldType == java.sql.Types.INTEGER) {
                    val intValue = resultSet.getInt(fieldName)
                    for (j in 1..(width+(fieldName.length-intValue.toString().length))) {
                        space.append(" ")
                    }
                    print("$space$intValue")
                } else {
                    val intString = resultSet.getString(fieldName)
                    for (j in 1..(width+(fieldName.length-intString.length))) {
                        space.append(" ")
                    }
                    print("$space$intString")
                }
            }
            println()
        }
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
