package simpledb.jdbc.embdded

import simpledb.jdbc.ConnectionAdapter
import simpledb.server.SimpleDB
import java.sql.SQLException
import java.sql.Statement

class EmbeddedConnection(
    private val db: SimpleDB,
) : ConnectionAdapter() {
    var currentTransaction = db.newTransaction()
    private val planner = db.planner

    override fun createStatement(): Statement {
        return EmbeddedStatement(this, planner)
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        throw SQLException("operation not implemented")
    }

    override fun close() {
        commit()
    }

    override fun commit() {
        currentTransaction.commit()
        currentTransaction = db.newTransaction()
    }

    override fun rollback() {
        currentTransaction.rollback()
        currentTransaction = db.newTransaction()
    }
}