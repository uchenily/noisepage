#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "execution/compiler/executable_query.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "taskflow/taskflow_util.h"

namespace noisepage::network {

/**
 * Statement is a postgres concept (see the Extended Query documentation:
 * https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
 * It owns the original query text that came across in the message parsed statement, the output from the Parser, and the
 * parameter types (if any).
 *
 * For caching purposes, it also takes ownership of the physical plan and the ExecutableQuery after code generation.
 * This allows for a single fingerprint to reference this prepared statement be bound and executed with different
 * parameters multiple times.
 */
class Statement {
public:
    /**
     * Constructor that doesn't have parameter types, i.e. Simple Query protocol
     * @param query_text original query text from the wire
     * @param parse_result output from postgresparser
     */
    Statement(std::string &&query_text, std::unique_ptr<parser::ParseResult> &&parse_result)
        : Statement(std::move(query_text), std::move(parse_result), {}) {}

    /**
     * Constructor that does have parameter types, i.e. Extended Query protocol
     * @param query_text original query text from the wire
     * @param parse_result output from postgresparser
     * @param param_types types of the values to be bound
     */
    Statement(std::string                            &&query_text,
              std::unique_ptr<parser::ParseResult>   &&parse_result,
              std::vector<execution::sql::SqlTypeId> &&param_types);

    /**
     * @return true if the statement is empty
     */
    auto Empty() const -> bool {
        return parse_result_->Empty();
    }

    /**
     * @return managed pointer to the output of the parser for this statement
     */
    auto ParseResult() const -> common::ManagedPointer<parser::ParseResult> {
        return common::ManagedPointer(parse_result_);
    }

    /**
     * @return managed pointer to the  root statement of the ParseResult. Just shorthand for
     * ParseResult->GetStatement(0)
     */
    auto RootStatement() const -> common::ManagedPointer<parser::SQLStatement> {
        return {root_statement_};
    }

    /**
     * @return vector of the statements parameters (if any)
     */
    auto ParamTypes() const -> const std::vector<execution::sql::SqlTypeId> & {
        return param_types_;
    }

    /**
     * @return QueryType of the root statement of the ParseResult
     */
    auto GetQueryType() const -> QueryType {
        return type_;
    }

    /**
     * @return the original query text. This is a const & instead of a std::string_view because we require that it be
     * null-terminated to pass the underlying C-string to libpgquery methods. std::string_view does not guarantee
     * null-termination. We could add a std::string_view accessor for performance if we can justify it.
     */
    auto GetQueryText() const -> const std::string & {
        return query_text_;
    }

    /**
     * @return the optimize result of the query
     */
    auto OptimizeResult() const -> common::ManagedPointer<optimizer::OptimizeResult> {
        return common::ManagedPointer(optimize_result_);
    }

    /**
     * @return the optimized physical plan for this query
     */
    auto PhysicalPlan() const -> common::ManagedPointer<planner::AbstractPlanNode> {
        return optimize_result_->GetPlanNode();
    }

    /**
     * @return the compiled executable query
     */
    auto GetExecutableQuery() const -> common::ManagedPointer<execution::compiler::ExecutableQuery> {
        return common::ManagedPointer(executable_query_);
    }

    /**
     * @param optimize_result optimize result to take ownership of
     */
    void SetOptimizeResult(std::unique_ptr<optimizer::OptimizeResult> &&optimize_result) {
        optimize_result_ = std::move(optimize_result);
    }

    /**
     * @param executable_query executable query to take ownership of
     */
    void SetExecutableQuery(std::unique_ptr<execution::compiler::ExecutableQuery> &&executable_query) {
        executable_query_ = std::move(executable_query);
    }

    /**
     * Stash desired parameter types to avoid having to do a full binding pass for prepared statements
     * @param desired_param_types output from the binder if Statement has parameters to fast-path convert for future
     * bindings
     */
    void SetDesiredParamTypes(std::vector<execution::sql::SqlTypeId> &&desired_param_types) {
        desired_param_types_ = std::move(desired_param_types);
        NOISEPAGE_ASSERT(desired_param_types_.size() == param_types_.size(), "");
    }

    /**

     * @return output from the binder if Statement has parameters to fast-path convert for future
     * bindings
     */
    auto GetDesiredParamTypes() const -> const std::vector<execution::sql::SqlTypeId> & {
        return desired_param_types_;
    }

    /**
     * Remove the cached objects related to query execution for this Statement. This should be done any time there is a
     * DDL change related to this statement.
     */
    void ClearCachedObjects() {
        optimize_result_ = nullptr;
        executable_query_ = nullptr;
        desired_param_types_ = {};
    }

private:
    const std::string                            query_text_;
    const std::unique_ptr<parser::ParseResult>   parse_result_ = nullptr;
    const std::vector<execution::sql::SqlTypeId> param_types_;
    common::ManagedPointer<parser::SQLStatement> root_statement_ = nullptr;
    enum QueryType                               type_ = QueryType::QUERY_INVALID;

    // The following objects can be "cached" in Statement objects for future statement invocations. Though they don't
    // relate to the Postgres Statement concept, these objects should be compatible with future queries that match the
    // same query text. The exception to this that DDL changes can break these cached objects.
    std::unique_ptr<optimizer::OptimizeResult>            optimize_result_ = nullptr;  // generated in the Bind phase
    std::unique_ptr<execution::compiler::ExecutableQuery> executable_query_ = nullptr; // generated in the Execute phase
    std::vector<execution::sql::SqlTypeId>                desired_param_types_;        // generated in the Bind phase
};

} // namespace noisepage::network
