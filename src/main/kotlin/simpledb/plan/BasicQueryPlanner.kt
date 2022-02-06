package simpledb.plan

import simpledb.metadata.MetadataManager
import simpledb.parse.Parser
import simpledb.parse.QueryData
import simpledb.tx.Transaction

class BasicQueryPlanner(
    private val metadataManager: MetadataManager,
) : QueryPlanner {
    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        // Step 1: Create a plan for each mentioned table or view.
        val plans = mutableListOf<Plan>()
        for (tableName in data.tables) {
            val viewDef = metadataManager.getViewDef(tableName, transaction)
            if (viewDef != null) {
                // Recursively plan the view
                val parser = Parser(viewDef)
                val viewData = parser.query()
                plans.add(createPlan(viewData, transaction))
            } else {
                plans.add(TablePlan(transaction, tableName, metadataManager))
            }
        }

        // Step 2: Create the product of all table plans.
        var plan = plans.removeAt(0)
        for (nextPlan in plans) {
            val productPlan = ProductPlan(plan, nextPlan)
        }

        // Step 3: Add a selection plan for the predicate
        plan = SelectPlan(plan, data.predicate)

        // Step 4: Project on the field names
        plan = ProjectPlan(plan, data.fields)
        return plan
    }
}