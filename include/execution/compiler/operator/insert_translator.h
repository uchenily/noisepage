#pragma once

#include <vector>

#include "execution/ast/identifier.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class Schema;
} // namespace noisepage::catalog

namespace noisepage::planner {
class InsertPlanNode;
} // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * InsertTranslator
 */
class InsertTranslator : public OperatorTranslator, public PipelineDriver {
public:
    /**
     * Create a new translator for the given insert plan. The compilation occurs within the
     * provided compilation context and the operator is participating in the provided pipeline.
     * @param plan The plan.
     * @param compilation_context The context of compilation this translation is occurring in.
     * @param pipeline The pipeline this operator is participating in.
     */
    InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

    /**
     * Does nothing.
     * @param decls The top-level declarations.
     */
    void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

    /**
     * Initialize the storage interface and counters.
     * @param pipeline The current pipeline.
     * @param function The pipeline generating function.
     */
    void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

    /**
     * Tear down the storage interface.
     * @param pipeline The current pipeline.
     * @param function The pipeline generating function.
     */
    void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

    /**
     * Implement insertion logic where it fills in the insert PR obtained from the StorageInterface struct
     * with values from the child.
     * @param context The context of the work.
     * @param function The pipeline generating function.
     */
    void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

    /**
     * Implement main insertion logic
     * @param context The context of the work.
     * @param function The pipeline generating function.
     * @param generate_set_table_pr function to generate the TPL that sets the fields of the projected row
     */
    void PerformInsertWork(WorkContext                                                 *context,
                           FunctionBuilder                                             *function,
                           const std::function<void(WorkContext *, FunctionBuilder *)> &generate_set_table_pr) const;

    /**
     * @return The child's output at the given index.
     */
    ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

    /**
     * @return An expression representing the value of the column with the given OID.
     */
    ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

    /** @return Throw an error, this is serial for now. */
    util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
        UNREACHABLE("Insert is serial.");
    };

    /** @return Throw an error, this is serial for now. */
    void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
        UNREACHABLE("Insert is serial.");
    };

private:
    /** Declare storage interface. */
    void DeclareInserter(FunctionBuilder *builder) const;

    /** Free the storage interface. */
    void GenInserterFree(FunctionBuilder *builder) const;

    /** Sets the oids that we are inserting on, using the schema from the insert plan node. */
    void SetOids(FunctionBuilder *builder) const;

    /** Declares the projected row that we will be using the insert values with. */
    void DeclareInsertPR(FunctionBuilder *builder) const;

    /** Gets the projected row pointer that we will fill in with values to insert. */
    void GetInsertPR(FunctionBuilder *builder) const;

    /** Sets the values in the projected row for a value insert which we will use to insert into the table. */
    void GenValueSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const;

    /** Sets the values in the projected row for an insert into select which we will use to insert into the table. */
    void GenSelectSetTablePR(FunctionBuilder *builder, WorkContext *context) const;

    /** Insert into the table. */
    void GenTableInsert(FunctionBuilder *builder) const;

    /** Insert into an index of this table. */
    void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

    /** Gets all the column oids in a schema. */
    static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema);

    // Storage interface for inserts.
    StateDescriptor::Entry si_inserter_;

    // Projected row that the inserter spits out for us to insert with.
    ast::Identifier insert_pr_;

    // Column oids that we are inserting on.
    ast::Identifier col_oids_;

    // Schema of the table that we are inserting on.
    const catalog::Schema &table_schema_;

    // All the oids that we are inserting on.
    std::vector<catalog::col_oid_t> all_oids_;

    // Projection map of the table that we are inserting into.
    // This maps column oids to offsets in a projected row.
    storage::ProjectionMap table_pm_;

    // The number of inserts that are performed.
    StateDescriptor::Entry num_inserts_;
};

} // namespace noisepage::execution::compiler
