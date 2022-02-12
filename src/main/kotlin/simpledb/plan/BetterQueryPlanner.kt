package simpledb.plan

import simpledb.metadata.MetadataManager
import simpledb.parse.Parser
import simpledb.parse.QueryData
import simpledb.tx.Transaction

class BetterQueryPlanner(
    private val metadataManager: MetadataManager,
) : QueryPlanner {
    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        // Step 1: Create a plan for each mentioned table or view.
        val plans = mutableListOf<Plan>()
        for (tableName in data.tables) {
            val viewdef = metadataManager.getViewDef(tableName, transaction)
            if (viewdef != null) {
                // Recursively plan the view.
                val parser = Parser(viewdef)
                val viewData = parser.query()
                plans.add(createPlan(viewData, transaction))
            } else {
                plans.add(TablePlan(transaction, tableName, metadataManager))
            }
        }

        // Step 2: Create the product of all table plans
        var plan = plans.removeAt(0)
        for (nextPlan in plans) {
            // Try both orderings and choose the one having lowest cost
            val choice1: Plan = ProductPlan(nextPlan, plan)
            val choice2: Plan = ProductPlan(plan, nextPlan)
            plan = if (choice1.blocksAccessed() < choice2.blocksAccessed()) {
                choice1
            } else {
                choice2
            }
        }

        // Step 3: Add a selection plan for the predicate
        plan = SelectPlan(plan, data.predicate)

        // Step 4: Project on the field names
        plan = ProjectPlan(plan, data.fields)
        return plan
    }
}