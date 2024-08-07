#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "optimizer/optimize_result.h"

namespace noisepage::catalog {
class CatalogAccessor;
} // namespace noisepage::catalog

namespace noisepage::parser {
class ConstantValueExpression;
class ParseResult;
class SQLStatement;
class SelectStatement;
} // namespace noisepage::parser

namespace noisepage::planner {
class AbstractPlanNode;
} // namespace noisepage::planner

namespace noisepage::optimizer {
class StatsStorage;
class AbstractCostModel;
class PropertySet;
} // namespace noisepage::optimizer

namespace noisepage::transaction {
class TransactionContext;
} // namespace noisepage::transaction

namespace noisepage::taskflow {

/**
 * Static helper methods for accessing some of the taskflow's functionality without instantiating an object
 */
class TaskflowUtil {
public:
    TaskflowUtil() = delete;

    /**
     * @param txn used by optimizer
     * @param accessor used by optimizer
     * @param query bound ParseResult
     * @param db_oid database oid
     * @param stats_storage used by optimizer
     * @param cost_model used by optimizer
     * @param optimizer_timeout used by optimizer
     * @param parameters parameters for the query, can be nullptr if there are no parameters
     * @return physical plan that can be executed
     */
    static auto Optimize(common::ManagedPointer<transaction::TransactionContext>              txn,
                         common::ManagedPointer<catalog::CatalogAccessor>                     accessor,
                         common::ManagedPointer<parser::ParseResult>                          query,
                         catalog::db_oid_t                                                    db_oid,
                         common::ManagedPointer<optimizer::StatsStorage>                      stats_storage,
                         std::unique_ptr<optimizer::AbstractCostModel>                        cost_model,
                         uint64_t                                                             optimizer_timeout,
                         common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters)
        -> std::unique_ptr<optimizer::OptimizeResult>;

    /**
     * Converts parser statement types (which rely on multiple enums) to a single QueryType enum from the network layer
     * @param statement
     * @return
     */
    static auto QueryTypeForStatement(common::ManagedPointer<parser::SQLStatement> statement) -> network::QueryType;

private:
    static void CollectSelectProperties(common::ManagedPointer<parser::SelectStatement> sel_stmt,
                                        optimizer::PropertySet                         *property_set);
};

} // namespace noisepage::taskflow
