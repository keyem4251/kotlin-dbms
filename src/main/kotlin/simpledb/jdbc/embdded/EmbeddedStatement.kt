package simpledb.jdbc.embdded

import simpledb.jdbc.StatementAdapter
import simpledb.plan.Plan
import simpledb.plan.Planner
import java.sql.ResultSet
import java.sql.SQLException

class EmbeddedStatement(
    private val connection: EmbeddedConnection,
    private val planner: Planner,
) : StatementAdapter() {
    override fun executeQuery(sql: String?): ResultSet {
        try {
            val transaction = connection.currentTransaction
            val cmd = sql ?: throw RuntimeException("null sql error")
            val plan: Plan = planner.createQueryPlan(cmd, transaction)
            return EmbeddedResultSet(plan, connection)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun executeUpdate(sql: String?): Int {
        try {
            val transaction = connection.currentTransaction
            val cmd = sql ?: throw RuntimeException("null sql error")
            val result = planner.executeUpdate(cmd, transaction)
            connection.commit()
            return result
        } catch (e: RuntimeException) {
            println(e)
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun close() {}
}