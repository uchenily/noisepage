#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "optimizer/abstract_optimizer.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/optimize_result.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/property_set.h"

namespace noisepage {

namespace planner {
    class AbstractPlanNode;
    class CteScanPlanNode;
    class PlanMetaData;
} // namespace planner

namespace catalog {
    class CatalogAccessor;
} // namespace catalog

namespace transaction {
    class TransactionContext;
} // namespace transaction

namespace optimizer {

    class OperatorNode;
    class PlanGenerator;

    /**
     * Optimizer class that implements the AbstractOptimizer abstract class
     */
    class Optimizer : public AbstractOptimizer {
    public:
        /**
         * Disallow copy and move
         */
        DISALLOW_COPY_AND_MOVE(Optimizer);

        /**
         * Constructor for Optimizer with a cost_model
         * @param model Cost Model to use for the optimizer
         * @param task_execution_timeout time in ms to spend on a task
         */
        explicit Optimizer(std::unique_ptr<AbstractCostModel> model, const uint64_t task_execution_timeout)
            : cost_model_(std::move(model))
            , context_(std::make_unique<OptimizerContext>(common::ManagedPointer(cost_model_)))
            , task_execution_timeout_(task_execution_timeout) {}

        /**
         * Build the plan tree for query execution
         * @param txn TransactionContext
         * @param accessor CatalogAccessor for catalog
         * @param storage StatsStorage
         * @param query_info Information about the query
         * @param op_tree Logical operator tree for execution
         * @param parameters parameters for the query, can be nullptr if there are no parameters
         * @returns execution plan
         */
        auto BuildPlanTree(transaction::TransactionContext                                     *txn,
                           catalog::CatalogAccessor                                            *accessor,
                           StatsStorage                                                        *storage,
                           QueryInfo                                                            query_info,
                           std::unique_ptr<AbstractOptimizerNode>                               op_tree,
                           common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters)
            -> std::unique_ptr<OptimizeResult> override;

        /**
         * Reset the optimizer state
         */
        void Reset() override;

    private:
        /**
         * Invoke a single optimization pass through the entire query.
         * The optimization pass includes rewriting and optimization logic.
         * @param root_group_id Group to begin optimization at
         * @param required_props Physical properties to enforce
         */
        void OptimizeLoop(group_id_t root_group_id, PropertySet *required_props);

        /**
         * Retrieve the lowest cost execution plan with the given properties
         *
         * @param txn TransactionContext
         * @param accessor CatalogAccessor
         * @param id ID of the group to produce the best physical operator
         * @param requirements Set of properties produced operator tree must satisfy
         * @param required_cols AbstractExpression tree output columns group must generate
         * @param plan_generator Plan generator
         * @returns Lowest cost plan
         */
        auto ChooseBestPlan(transaction::TransactionContext                                       *txn,
                            catalog::CatalogAccessor                                              *accessor,
                            group_id_t                                                             id,
                            PropertySet                                                           *required_props,
                            const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
                            PlanGenerator *plan_generator) -> std::unique_ptr<planner::AbstractPlanNode>;

        /**
         * Execute elements of given optimization task stack and ensure that we
         * do not go beyond the time limit (unless if one plan has not been
         * generated yet)
         *
         * @param task_stack Optimizer's Task Stack to execute through
         * @param root_group_id Root Group ID to check whether there is a plan or not
         * @param root_context OptimizerContext to use that maintains required properties
         */
        void
        ExecuteTaskStack(OptimizerTaskStack *task_stack, group_id_t root_group_id, OptimizationContext *root_context);

        /**
         * Invoke a single DFS pass through the entire plan
         * tree to assign the CTE leader node and also provide each
         * CTE Plan node with the cte table schema
         */
        void ElectCTELeader(common::ManagedPointer<planner::AbstractPlanNode> plan,
                            catalog::table_oid_t                              table_oid,
                            common::ManagedPointer<planner::CteScanPlanNode> *leader);

        std::unique_ptr<AbstractCostModel> cost_model_;
        std::unique_ptr<OptimizerContext>  context_;
        const uint64_t                     task_execution_timeout_;
    };

} // namespace optimizer
} // namespace noisepage
