#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/compiler/codegen.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/pipeline.h"

namespace noisepage::execution::compiler {
class CompilerSettings;
} // namespace noisepage::execution::compiler

namespace noisepage::parser {
class AbstractExpression;
} // namespace noisepage::parser

namespace noisepage::planner {
class AbstractPlanNode;
class PlanMetaData;
} // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * An enumeration capturing the mode of code generation when compiling SQL queries to TPL.
 */
enum class CompilationMode : uint8_t {
    // One-shot compilation is when all TPL code for the whole query plan is
    // generated up-front at once before execution.
    OneShot,

    // Interleaved compilation is a mode that mixes code generation and execution.
    // Query fragments are generated and executed in lock step.
    Interleaved,
};

/**
 * Context of all code-generation related structures. A temporary container that lives only during
 * code generation storing:
 * - Translators for relational operators
 * - Translators for expressions
 * - Pipelines making up the query plan.
 */
class CompilationContext {
public:
    /**
     * Compile the given plan into an executable query.
     * @param plan The plan to compile.
     * @param exec_settings The execution settings to be used for compilation.
     * @param accessor The catalog accessor to use for compilation.
     * @param mode The compilation mode.
     * @param override_qid Optional indicating how to override the plan's query id
     * @param plan_meta_data Query plan meta data (stores cardinality information)
     */
    static std::unique_ptr<ExecutableQuery> Compile(const planner::AbstractPlanNode &plan,
                                                    const exec::ExecutionSettings   &exec_settings,
                                                    catalog::CatalogAccessor        *accessor,
                                                    CompilationMode mode = CompilationMode::Interleaved,
                                                    std::optional<execution::query_id_t> override_qid = std::nullopt,
                                                    common::ManagedPointer<planner::PlanMetaData> plan_meta_data
                                                    = nullptr);

    /**
     * Register a pipeline in this context.
     * @param pipeline The pipeline.
     * @return A unique ID for the pipeline in this context.
     */
    uint32_t RegisterPipeline(Pipeline *pipeline);

    /**
     * Prepare compilation for the given relational plan node participating in the provided pipeline.
     * @param plan The plan node.
     * @param pipeline The pipeline the node belongs to.
     */
    void Prepare(const planner::AbstractPlanNode &plan, Pipeline *pipeline);

    /**
     * Prepare compilation for the given expression.
     * @param expression The expression.
     */
    void Prepare(const parser::AbstractExpression &expression);

    /**
     * @return The code generator instance.
     */
    CodeGen *GetCodeGen() {
        return &codegen_;
    }

    /**
     * @return The query state.
     */
    StateDescriptor *GetQueryState() {
        return &query_state_;
    }

    /**
     * @return The translator for the given relational plan node; null if the provided plan node does
     *         not have a translator registered in this context.
     */
    OperatorTranslator *LookupTranslator(const planner::AbstractPlanNode &node) const;

    /**
     * @return The translator for the given expression; null if the provided expression does not have
     *         a translator registered in this context.
     */
    ExpressionTranslator *LookupTranslator(const parser::AbstractExpression &expr) const;

    /**
     * @return A common prefix for all functions generated in this module.
     */
    std::string GetFunctionPrefix() const;

    /**
     * @return The list of parameters common to all query functions. For now, just the query state.
     */
    util::RegionVector<ast::FieldDecl *> QueryParams() const;

    /**
     * @return The slot in the query state where the execution context can be found.
     */
    ast::Expr *GetExecutionContextPtrFromQueryState();

    /**
     * @return The compilation mode.
     */
    CompilationMode GetCompilationMode() const {
        return mode_;
    }

    /** @return True if we should collect counters in TPL, used for Lin's models. */
    bool IsCountersEnabled() const {
        return counters_enabled_;
    }

    /** @return True if we should record pipeline metrics */
    bool IsPipelineMetricsEnabled() const {
        return pipeline_metrics_enabled_;
    }

    /** @return Query Id associated with the query */
    query_id_t GetQueryId() const {
        return query_id_;
    }

private:
    // Private to force use of static Compile() function.
    explicit CompilationContext(ExecutableQuery               *query,
                                query_id_t                     query_id_,
                                catalog::CatalogAccessor      *accessor,
                                CompilationMode                mode,
                                const exec::ExecutionSettings &exec_settings);

    // Given a plan node, compile it into a compiled query object.
    void GeneratePlan(const planner::AbstractPlanNode              &plan,
                      common::ManagedPointer<planner::PlanMetaData> plan_meta_data);

    // Generate the query initialization function.
    ast::FunctionDecl *GenerateInitFunction();

    // Generate the query tear-down function.
    ast::FunctionDecl *GenerateTearDownFunction();

    // Prepare compilation for the output.
    void PrepareOut(const planner::AbstractPlanNode &plan, Pipeline *pipeline);

private:
    // Unique ID used as a prefix for all generated functions to ensure uniqueness.
    uint32_t unique_id_;

    // Query ID generated from ExecutableQuery or overridden specifically
    query_id_t query_id_;

    // The compiled query object we'll update.
    ExecutableQuery *query_;

    // The compilation mode.
    CompilationMode mode_;

    // The code generator instance.
    CodeGen codegen_;

    // Cached identifiers.
    ast::Identifier query_state_var_;
    ast::Identifier query_state_type_;

    // The query state and the slot in the state where the execution context is.
    StateDescriptor        query_state_;
    StateDescriptor::Entry exec_ctx_;

    // The operator and expression translators.
    std::unordered_map<const planner::AbstractPlanNode *, std::unique_ptr<OperatorTranslator>>    ops_;
    std::unordered_map<const parser::AbstractExpression *, std::unique_ptr<ExpressionTranslator>> expressions_;

    // The pipelines in this context in no specific order.
    std::vector<Pipeline *> pipelines_;

    // Whether counters are enabled.
    bool counters_enabled_;

    // Whether pipeline metrics are enabled.
    bool pipeline_metrics_enabled_;
};

} // namespace noisepage::execution::compiler
