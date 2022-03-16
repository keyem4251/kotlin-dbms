package simpledb.opt

import simpledb.metadata.MetadataManager
import simpledb.parse.QueryData
import simpledb.plan.Plan
import simpledb.plan.Planner
import simpledb.plan.ProjectPlan
import simpledb.plan.QueryPlanner
import simpledb.tx.Transaction

class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager,
) : QueryPlanner {
    private val tablePlanners = mutableListOf<TablePlanner>()

    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        // Step 1, Create a TablePlanner object for each mentioned table
        for (tableName in data.tables) {
            val tablePlanner = TablePlanner(tableName, data.predicate, transaction, metadataManager)
            tablePlanners.add(tablePlanner)
        }

        // Step 2, Choose the lowest-size plan to begin the join order
        var currentPlan = getLowestSelectPlan()

        // Step 3, Repeatedly add a plan to the join order
        while (tablePlanners.isNotEmpty()) {
            val plan = getLowestJoinPlan(currentPlan)
            currentPlan = if (plan != null) {
                plan
            } else {
                getLowestProductPlan(currentPlan)
            }
        }

        // Step 4, Project on the field names and return
        return ProjectPlan(currentPlan, data.fields)
    }

    private fun getLowestSelectPlan(): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeSelectPlan()
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tablePlanner
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan ?: throw RuntimeException("null error")
    }

    private fun getLowestJoinPlan(current: Plan): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeJoinPlan(current) ?: throw RuntimeException("null error")
            if (bestPlan != null && (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput())) {
                bestTablePlanner = tablePlanner
                bestPlan = plan
            }
        }
        if (bestPlan != null) tablePlanners.remove(bestTablePlanner)
        return bestPlan ?: throw RuntimeException("null error")
    }

    private fun getLowestProductPlan(current: Plan): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeProductPlan(current)
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tablePlanner
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan ?: throw RuntimeException("null error")
    }

    fun setPlanner(planner: Planner) {
        // for use in planning views, which for simplicity this code doesn't do.
    }
}