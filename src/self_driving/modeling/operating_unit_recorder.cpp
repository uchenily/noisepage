#include "self_driving/modeling/operating_unit_recorder.h"

#include <utility>

#include "catalog/catalog_accessor.h"
#include "execution/ast/ast.h"
#include "execution/ast/context.h"
#include "execution/compiler/operator/hash_aggregation_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/operator/index_create_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/operator/sort_translator.h"
#include "execution/compiler/operator/static_aggregation_translator.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/hash_table_entry.h"
#include "execution/sql/sql.h"
#include "execution/sql/table_vector_iterator.h"
#include "optimizer/index_util.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/cte_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/plan_meta_data.h"
#include "planner/plannodes/plan_visitor.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/modeling/operating_unit_util.h"
#include "storage/block_layout.h"
#include "storage/index/bplustree.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::selfdriving {

auto OperatingUnitRecorder::DeriveIndexBuildThreads(const catalog::IndexSchema &schema, catalog::table_oid_t tbl_oid)
    -> size_t {
    size_t num_threads = 1;
    auto  &options = schema.GetIndexOptions().GetOptions();
    if (options.find(catalog::IndexOptions::Knob::BUILD_THREADS) != options.end()) {
        auto expr = options.find(catalog::IndexOptions::Knob::BUILD_THREADS)->second.get();
        auto cve = reinterpret_cast<parser::ConstantValueExpression *>(expr);
        num_threads = cve->Peek<int32_t>();
    }

    auto   sql_table = accessor_->GetTable(tbl_oid);
    size_t num_tasks
        = std::ceil(sql_table->GetNumBlocks() * 1.0 / execution::sql::TableVectorIterator::K_MIN_BLOCK_RANGE_SIZE);
    return std::min(num_tasks, num_threads);
}

auto OperatingUnitRecorder::DeriveIndexSpecificFeatures(const catalog::IndexSchema &schema)
    -> std::pair<size_t, size_t> {
    if (schema.Type() != storage::index::IndexType::BPLUSTREE) {
        return std::make_pair(0, 0);
    }

    // For BPLUSTREE, feature0 is the upper threshold and feature1 is the lower threshold
    size_t feature0 = 0;
    size_t feature1 = 0;
    auto  &options = schema.GetIndexOptions().GetOptions();
    if (options.find(catalog::IndexOptions::Knob::BPLUSTREE_INNER_NODE_UPPER_THRESHOLD) != options.end()) {
        auto expr = options.find(catalog::IndexOptions::Knob::BPLUSTREE_INNER_NODE_UPPER_THRESHOLD)->second.get();
        auto cve = reinterpret_cast<parser::ConstantValueExpression *>(expr);
        feature0 = cve->Peek<int32_t>();
    } else {
        feature0 = storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD;
    }

    if (options.find(catalog::IndexOptions::Knob::BPLUSTREE_INNER_NODE_LOWER_THRESHOLD) != options.end()) {
        auto expr = options.find(catalog::IndexOptions::Knob::BPLUSTREE_INNER_NODE_LOWER_THRESHOLD)->second.get();
        auto cve = reinterpret_cast<parser::ConstantValueExpression *>(expr);
        feature1 = cve->Peek<int32_t>();
    } else {
        feature1 = storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD;
    }

    return std::make_pair(feature0, feature1);
}

template <typename IndexPlanNode>
void OperatingUnitRecorder::RecordIndexOperations(const std::vector<catalog::index_oid_t> &index_oids,
                                                  catalog::table_oid_t                     table_oid) {
    selfdriving::ExecutionOperatingUnitType type;
    if (std::is_same<IndexPlanNode, planner::InsertPlanNode>::value) {
        type = selfdriving::ExecutionOperatingUnitType::INDEX_INSERT;
    } else if (std::is_same<IndexPlanNode, planner::DeletePlanNode>::value) {
        type = selfdriving::ExecutionOperatingUnitType::INDEX_DELETE;
    } else if (std::is_same<IndexPlanNode, planner::UpdatePlanNode>::value) {
        // UPDATE is done as a DELETE followed by INSERT
        RecordIndexOperations<planner::InsertPlanNode>(index_oids, table_oid);
        RecordIndexOperations<planner::DeletePlanNode>(index_oids, table_oid);
        return;
    } else {
        NOISEPAGE_ASSERT(false, "Recording index operations for non-modiying plan node");
    }

    for (auto &oid : index_oids) {
        auto &index_schema = accessor_->GetIndexSchema(oid);
        auto  features = DeriveIndexSpecificFeatures(index_schema);

        // TODO(lin): Use the table size instead of the index size as the estiamte (since there may be "what-if" indexes
        //  that we don't populate). We probably need to use the stats if the pilot is not running on the primary.
        auto table = accessor_->GetTable(table_oid);

        std::vector<catalog::indexkeycol_oid_t> keys;
        size_t                                  num_rows = table->GetNumTuple();
        size_t                                  num_keys = index_schema.GetColumns().size();
        size_t key_size = ComputeKeySize(common::ManagedPointer(&index_schema), false, keys, &num_keys);

        // TODO(wz2): Eventually need to scale cardinality to consider num_rows being inserted/updated/deleted
        size_t car = 1;
        pipeline_features_.emplace(type,
                                   ExecutionOperatingUnitFeature(current_translator_->GetTranslatorId(),
                                                                 type,
                                                                 num_rows,
                                                                 key_size,
                                                                 num_keys,
                                                                 car,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 features.first,
                                                                 features.second));
    }
}

auto OperatingUnitRecorder::ComputeMemoryScaleFactor(execution::ast::StructDecl *decl,
                                                     size_t                      total_offset,
                                                     size_t                      key_size,
                                                     size_t                      ref_offset) -> double {
    auto *struct_type = reinterpret_cast<execution::ast::StructTypeRepr *>(decl->TypeRepr());
    auto &fields = struct_type->Fields();

    // Rough loop to get an estimate size of entire struct
    size_t total = total_offset;
    for (auto *field : fields) {
        auto *field_repr = field->TypeRepr();
        if (field_repr->GetType() != nullptr) {
            total += field_repr->GetType()->GetSize();
        } else if (execution::ast::IdentifierExpr::classof(field_repr)) {
            // Likely built in type
            auto *builtin_type
                = ast_ctx_->LookupBuiltinType(reinterpret_cast<execution::ast::IdentifierExpr *>(field_repr)->Name());
            if (builtin_type != nullptr) {
                total += builtin_type->GetSize();
            }
        }
    }

    // For mini-runners, only ints for sort, hj, aggregate
    double num_keys = key_size / 4.0;
    double ref_payload = (num_keys * sizeof(execution::sql::Integer)) + ref_offset;
    return total / ref_payload;
}

void OperatingUnitRecorder::AdjustKeyWithType(execution::sql::SqlTypeId type, size_t *key_size, size_t *num_key) {
    if (type == execution::sql::SqlTypeId::Varchar) {
        // TODO(lin): Some how varchar in execution engine is 24 bytes. I don't really know why, but just special case
        //  here since it's different than the storage size (16 bytes under inline)
        *key_size = *key_size + 24;
        *num_key = *num_key + 1;
    } else {
        *key_size = *key_size + storage::AttrSizeBytes(execution::sql::GetSqlTypeIdSize(type));
    }
}

auto OperatingUnitRecorder::ComputeKeySize(const std::vector<common::ManagedPointer<parser::AbstractExpression>> &exprs,
                                           size_t *num_key) -> size_t {
    size_t key_size = 0;
    for (auto &expr : exprs) {
        AdjustKeyWithType(expr->GetReturnValueType(), &key_size, num_key);
    }

    // The set of expressions represented by exprs should have some key size
    // that is non-zero.
    NOISEPAGE_ASSERT(key_size > 0, "KeySize must be greater than 0");
    return key_size;
}

auto OperatingUnitRecorder::ComputeKeySizeOutputSchema(const planner::AbstractPlanNode *plan, size_t *num_key)
    -> size_t {
    size_t key_size = 0;
    for (auto &col : plan->GetOutputSchema()->GetColumns()) {
        AdjustKeyWithType(col.GetType(), &key_size, num_key);
    }

    return key_size;
}

auto OperatingUnitRecorder::ComputeKeySize(catalog::table_oid_t tbl_oid, size_t *num_key) -> size_t {
    size_t key_size = 0;
    auto  &schema = accessor_->GetSchema(tbl_oid);
    for (auto &col : schema.GetColumns()) {
        AdjustKeyWithType(col.Type(), &key_size, num_key);
    }

    // We should select some columns from the table specified by tbl_oid.
    // Thus we assert that key_size > 0.
    NOISEPAGE_ASSERT(key_size > 0, "KeySize must be greater than 0");
    return key_size;
}

auto OperatingUnitRecorder::ComputeKeySize(catalog::table_oid_t                   tbl_oid,
                                           const std::vector<catalog::col_oid_t> &cols,
                                           size_t                                *num_key) -> size_t {
    size_t key_size = 0;
    auto  &schema = accessor_->GetSchema(tbl_oid);
    for (auto &oid : cols) {
        auto &col = schema.GetColumn(oid);
        AdjustKeyWithType(col.Type(), &key_size, num_key);
    }

    // We should select some columns from the table specified by tbl_oid.
    // Thus we assert that key_size > 0.
    NOISEPAGE_ASSERT(key_size > 0, "KeySize must be greater than 0");
    return key_size;
}

auto OperatingUnitRecorder::ComputeKeySize(common::ManagedPointer<const catalog::IndexSchema> schema,
                                           bool                                               restrict_cols,
                                           const std::vector<catalog::indexkeycol_oid_t>     &cols,
                                           size_t                                            *num_key) -> size_t {
    std::unordered_set<catalog::indexkeycol_oid_t> kcols;
    for (auto &col : cols) {
        kcols.insert(col);
    }

    size_t key_size = 0;
    for (auto &col : schema->GetColumns()) {
        if (!restrict_cols || kcols.find(col.Oid()) != kcols.end()) {
            AdjustKeyWithType(col.Type(), &key_size, num_key);
        }
    }

    return key_size;
}

auto OperatingUnitRecorder::ComputeKeySize(catalog::index_oid_t                           idx_oid,
                                           const std::vector<catalog::indexkeycol_oid_t> &cols,
                                           size_t                                        *num_key) -> size_t {
    auto &schema = accessor_->GetIndexSchema(idx_oid);
    return ComputeKeySize(common::ManagedPointer(&schema), true, cols, num_key);
}

void OperatingUnitRecorder::AggregateFeatures(selfdriving::ExecutionOperatingUnitType type,
                                              size_t                                  key_size,
                                              size_t                                  num_keys,
                                              const planner::AbstractPlanNode        *plan,
                                              size_t                                  scaling_factor,
                                              double                                  mem_factor) {
    size_t num_rows;
    // TODO(lin): Some times cardinality represents the number of distinct values for some OUs (e.g., SORT_BUILD),
    //  but we don't have a good way to estimate that right now. So in those cases we just copy num_rows
    size_t cardinality;
    size_t num_loops = 0;
    size_t num_concurrent = 0; // the number of concurrently executing threads (issue #1241)

    size_t specific_feature0 = 0;
    size_t specific_feature1 = 0;

    // Dummy default values in case we don't have stats
    size_t current_plan_cardinality = 1;
    size_t table_num_rows = 1;
    if (plan_meta_data_ != nullptr) {
        auto &plan_node_meta_data = plan_meta_data_->GetPlanNodeMetaData(plan->GetPlanNodeId());
        current_plan_cardinality = plan_node_meta_data.GetCardinality();
        table_num_rows = plan_node_meta_data.GetTableNumRows();
    }

    switch (type) {
    case ExecutionOperatingUnitType::OUTPUT: {
        num_rows = current_plan_cardinality;
        if (accessor_->GetDatabaseOid("tpch_runner_db") != catalog::INVALID_DATABASE_OID) {
            // Unfortunately we don't know what kind of output callback that we're going to call at runtime, so we just
            // special case this when we execute the plans directly from the TPCH runner and use the NoOpResultConsumer
            cardinality = 0;
        } else {
            // Uses the network result consumer
            cardinality = 1;
        }
        auto child_translator = current_translator_->GetChildTranslator();
        if (child_translator != nullptr) {
            if (child_translator->Op()->GetPlanNodeType() == planner::PlanNodeType::PROJECTION) {
                auto output = child_translator->Op()->GetOutputSchema()->GetColumn(0).GetExpr();
                if (output && output->GetExpressionType() == parser::ExpressionType::FUNCTION) {
                    auto f_expr = output.CastTo<const parser::FunctionExpression>();
                    if (f_expr->GetFuncName() == "nprunnersemitint" || f_expr->GetFuncName() == "nprunnersemitreal") {
                        auto child = f_expr->GetChild(0);
                        NOISEPAGE_ASSERT(child, "NpRunnersEmit should have children");
                        NOISEPAGE_ASSERT(child->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT,
                                         "Child should be constants");

                        auto cve = child.CastTo<const parser::ConstantValueExpression>();
                        num_rows = cve->GetInteger().val_;
                    }
                }
            }
        }
    } break;
    case ExecutionOperatingUnitType::HASHJOIN_PROBE: {
        NOISEPAGE_ASSERT(plan->GetPlanNodeType() == planner::PlanNodeType::HASHJOIN, "HashJoin plan expected");
        cardinality = current_plan_cardinality; // extract from plan num_rows (# matched rows)

        if (plan_meta_data_ != nullptr) {
            auto *c_plan = plan->GetChild(1);
            // extract from c_plan num_rows (# row to probe)
            num_rows = plan_meta_data_->GetPlanNodeMetaData(c_plan->GetPlanNodeId()).GetCardinality();
        } else {
            num_rows = 1;
        }
    } break;
    case ExecutionOperatingUnitType::IDX_SCAN: {
        // For IDX_SCAN, the feature is as follows:
        // - num_rows is the size of the index
        // - cardinality is the scan size
        catalog::table_oid_t table_oid;
        catalog::index_oid_t index_oid;
        if (plan->GetPlanNodeType() == planner::PlanNodeType::INDEXSCAN) {
            auto index_scan_plan = reinterpret_cast<const planner::IndexScanPlanNode *>(plan);
            index_oid = index_scan_plan->GetIndexOid();
            table_oid = index_scan_plan->GetTableOid();
            if (table_num_rows == 0) {
                // When we didn't run Analyze
                num_rows = index_scan_plan->GetIndexSize();
            } else {
                num_rows = table_num_rows;
            }

            const auto &idx_schema = accessor_->GetIndexSchema(index_oid);
            std::tie(specific_feature0, specific_feature1) = DeriveIndexSpecificFeatures(idx_schema);

            std::vector<catalog::col_oid_t>                                    mapped_cols;
            std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> lookup;

            [[maybe_unused]] bool status = optimizer::IndexUtil::ConvertIndexKeyOidToColOid(accessor_.Get(),
                                                                                            table_oid,
                                                                                            idx_schema,
                                                                                            &lookup,
                                                                                            &mapped_cols);
            NOISEPAGE_ASSERT(status, "Failed to get index key oids in operating unit recorder");

            if (plan_meta_data_ != nullptr) {
                cardinality = table_num_rows; // extract from plan num_rows (this is the scan size)
                auto  &plan_node_meta_data = plan_meta_data_->GetPlanNodeMetaData(plan->GetPlanNodeId());
                double selectivity = 1;
                for (auto col_id : mapped_cols) {
                    selectivity *= plan_node_meta_data.GetFilterColumnSelectivity(col_id);
                }
                cardinality = cardinality * selectivity;
            } else {
                cardinality = 1;
            }
        } else {
            NOISEPAGE_ASSERT(plan->GetPlanNodeType() == planner::PlanNodeType::INDEXNLJOIN, "Expected IdxJoin");
            auto index_join_plan = reinterpret_cast<const planner::IndexJoinPlanNode *>(plan);

            if (plan_meta_data_ != nullptr) {
                auto *c_plan = plan->GetChild(0);
                // extract from c_plan num_row
                num_loops = plan_meta_data_->GetPlanNodeMetaData(c_plan->GetPlanNodeId()).GetCardinality();
            }

            const auto &idx_schema = accessor_->GetIndexSchema(index_join_plan->GetIndexOid());
            std::tie(specific_feature0, specific_feature1) = DeriveIndexSpecificFeatures(idx_schema);

            // FIXME(lin): Right now we do not populate the cardinality or selectivity stats for the inner index scan of
            //  INDEXNLJOIN. We directly get the size from the index and assume the inner index scan only returns 1
            //  tuple.
            num_rows = index_join_plan->GetIndexSize();
            cardinality = 1;
        }
    } break;
    case ExecutionOperatingUnitType::SEQ_SCAN: {
        num_rows = table_num_rows;
        cardinality = table_num_rows;
    } break;
    case ExecutionOperatingUnitType::SORT_TOPK_BUILD: {
        num_rows = current_plan_cardinality;
        // TODO(lin): This should be the limit size for the OrderByPlanNode, which is the parent plan for the plan of
        //  the SORT_TOPK_BUILD OU. We need to refactor the interface to pass in this information correctly.
        cardinality = 1;
    } break;
    case ExecutionOperatingUnitType::CREATE_INDEX_MAIN: {
        // This OU does not record a num_concurrent. For this part, we only ensure that the
        // num_rows and cardinality are set accordingly. This should be sufficient to allow
        // downstream consumer to use this feature for prediction.
        num_rows = table_num_rows;
        cardinality = table_num_rows;
    } break;
    case ExecutionOperatingUnitType::CREATE_INDEX: {
        num_rows = table_num_rows;
        cardinality = table_num_rows;
        // We extract the num_rows and cardinality from the table name if possible
        // This is a special case for mini-runners
        std::string idx_name = reinterpret_cast<const planner::CreateIndexPlanNode *>(plan)->GetIndexName();
        auto        mrpos = idx_name.find("minirunners__");
        if (mrpos != std::string::npos) {
            num_rows = atoi(idx_name.c_str() + mrpos + sizeof("minirunners__") - 1);
            cardinality = num_rows;
        }

        const auto &idx_schema = reinterpret_cast<const planner::CreateIndexPlanNode *>(plan)->GetSchema();
        std::tie(specific_feature0, specific_feature1) = DeriveIndexSpecificFeatures(*idx_schema);

        size_t num_threads
            = DeriveIndexBuildThreads(*idx_schema,
                                      reinterpret_cast<const planner::CreateIndexPlanNode *>(plan)->GetTableOid());

        // Adjust the CREATE_INDEX OU by the concurrent estimate. DeriveIndexBuildThreads takes
        // into consideration the number of threads available and the number of blocks the
        // table has. The num_rows/cardinality is scaled accordingly for now and num_concurrent
        // is set to the estimate.
        num_rows /= num_threads;
        cardinality /= num_threads;
        num_concurrent = num_threads;
    } break;
    default:
        num_rows = current_plan_cardinality;
        cardinality = current_plan_cardinality;
    }

    // Setting the cardinality to at least to one in case losing accuracy during casting. We don't model the case when
    // the cardinality is 0 either.
    num_rows = std::max(num_rows, 1lu);
    cardinality = std::max(cardinality, 1lu);

    num_rows *= scaling_factor;
    cardinality *= scaling_factor;

    // This is a hack.
    // Certain translators don't own their features, but pass them further down the pipeline.
    std::vector<execution::translator_id_t>                         translator_ids;
    common::ManagedPointer<execution::compiler::OperatorTranslator> translator = current_translator_;
    translator_ids.emplace_back(translator->GetTranslatorId());
    while (translator->GetParentTranslator() != nullptr && translator->IsCountersPassThrough()) {
        translator = translator->GetParentTranslator();
        translator_ids.emplace_back(translator->GetTranslatorId());
    }

    auto itr_pair = pipeline_features_.equal_range(type);
    for (auto itr = itr_pair.first; itr != itr_pair.second; itr++) {
        NOISEPAGE_ASSERT(itr->second.GetExecutionOperatingUnitType() == type, "multimap consistency failure");
        bool same_translator = std::find(translator_ids.cbegin(), translator_ids.cend(), itr->second.GetTranslatorId())
                               != translator_ids.cend();
        if (itr->second.GetKeySize() == key_size && itr->second.GetNumKeys() == num_keys
            && OperatingUnitUtil::IsOperatingUnitTypeMergeable(type) && same_translator) {
            itr->second.SetNumRows(num_rows + itr->second.GetNumRows());
            itr->second.SetCardinality(cardinality + itr->second.GetCardinality());
            itr->second.AddMemFactor(mem_factor);
            return;
        }
    }

    auto feature = ExecutionOperatingUnitFeature(translator->GetTranslatorId(),
                                                 type,
                                                 num_rows,
                                                 key_size,
                                                 num_keys,
                                                 cardinality,
                                                 mem_factor,
                                                 num_loops,
                                                 num_concurrent,
                                                 specific_feature0,
                                                 specific_feature1);
    pipeline_features_.emplace(type, std::move(feature));
}

void OperatingUnitRecorder::RecordArithmeticFeatures(const planner::AbstractPlanNode *plan, size_t scaling) {
    for (auto &feature : arithmetic_feature_types_) {
        NOISEPAGE_ASSERT(feature.second > ExecutionOperatingUnitType::PLAN_OPS_DELIMITER,
                         "Expected computation operator");
        if (feature.second != ExecutionOperatingUnitType::INVALID) {
            // Recording of simple operators
            // - num_keys is always 1
            // - key_size is max() inputs
            auto size = storage::AttrSizeBytes(execution::sql::GetSqlTypeIdSize(feature.first));
            AggregateFeatures(feature.second, size, 1, plan, scaling, 1);
        }
    }

    arithmetic_feature_types_.clear();
}

void OperatingUnitRecorder::VisitAbstractPlanNode(const planner::AbstractPlanNode *plan) {
    auto schema = plan->GetOutputSchema();
    if (schema != nullptr) {
        for (auto &column : schema->GetColumns()) {
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(column.GetExpr());
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));
        }
    }
}

void OperatingUnitRecorder::VisitAbstractScanPlanNode(const planner::AbstractScanPlanNode *plan) {
    VisitAbstractPlanNode(plan);

    if (plan->GetScanPredicate() != nullptr) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(plan->GetScanPredicate());
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));
    }
}

void OperatingUnitRecorder::Visit(const planner::CreateIndexPlanNode *plan) {
    std::vector<catalog::indexkeycol_oid_t> keys;

    auto   schema = plan->GetSchema();
    size_t num_keys = schema->GetColumns().size();
    size_t key_size
        = ComputeKeySize(common::ManagedPointer<const catalog::IndexSchema>(schema.Get()), false, keys, &num_keys);

    // TODO(lin): further adjust the key size of VARCHAR keys for CREATE INDEX becuase of some irregularity in the
    //  training data. Needs further investigation.
    for (auto &col : schema->GetColumns()) {
        if (col.Type() == execution::sql::SqlTypeId::Varchar) {
            key_size += 8;
        }
    }

    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);

    auto translator = current_translator_.CastTo<execution::compiler::IndexCreateTranslator>();
    if (translator->GetPipeline()->IsParallel()) {
        // Only want to record CREATE_INDEX_MAIN if the operator executes in parallel
        AggregateFeatures(ExecutionOperatingUnitType::CREATE_INDEX_MAIN, key_size, num_keys, plan, 1, 1);
    }
}

void OperatingUnitRecorder::Visit(const planner::SeqScanPlanNode *plan) {
    VisitAbstractScanPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // For a sequential scan:
    // - # keys is how mahy columns are scanned (either # cols in table OR plan->GetColumnOids().size()
    // - Total key size is the size of the columns scanned
    size_t key_size;
    size_t num_keys = 0;
    if (!plan->GetColumnOids().empty()) {
        key_size = ComputeKeySize(plan->GetTableOid(), plan->GetColumnOids(), &num_keys);
        num_keys = plan->GetColumnOids().size();
    } else {
        auto &schema = accessor_->GetSchema(plan->GetTableOid());
        num_keys = schema.GetColumns().size();
        key_size = ComputeKeySize(plan->GetTableOid(), &num_keys);
    }

    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::IndexScanPlanNode *plan) {
    std::unordered_set<catalog::indexkeycol_oid_t> cols;
    for (auto &pair : plan->GetLoIndexColumns()) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        cols.insert(pair.first);
    }

    for (auto &pair : plan->GetHiIndexColumns()) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        cols.insert(pair.first);
    }

    // Record operator features
    VisitAbstractScanPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // For an index scan, # keys is the number of columns in the key lookup
    // The total key size is the size of the key being used to lookup
    std::vector<catalog::indexkeycol_oid_t> col_vec;
    col_vec.reserve(cols.size());
    for (auto col : cols) {
        col_vec.emplace_back(col);
    }

    size_t num_keys = col_vec.size();
    size_t key_size = ComputeKeySize(plan->GetIndexOid(), col_vec, &num_keys);
    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::VisitAbstractJoinPlanNode(const planner::AbstractJoinPlanNode *plan) {
    if (plan_feature_type_ == ExecutionOperatingUnitType::HASHJOIN_PROBE
        || plan_feature_type_ == ExecutionOperatingUnitType::DUMMY) {
        // Right side stitches together outputs
        VisitAbstractPlanNode(plan);
    }
}

void OperatingUnitRecorder::Visit(const planner::HashJoinPlanNode *plan) {
    auto translator = current_translator_.CastTo<execution::compiler::HashJoinTranslator>();

    // Build
    if (translator->IsLeftPipeline(*current_pipeline_)) {
        for (auto key : plan->GetLeftHashKeys()) {
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));
        }

        // Get Struct and compute memory scaling factor
        auto offset = sizeof(execution::sql::HashTableEntry);
        auto num_key = plan->GetLeftHashKeys().size();
        auto key_size = ComputeKeySize(plan->GetLeftHashKeys(), &num_key);
        auto scale = ComputeMemoryScaleFactor(translator->GetStructDecl(), offset, key_size, offset);

        // Record features using the row/cardinality of left plan
        auto *c_plan = plan->GetChild(0);
        RecordArithmeticFeatures(c_plan, 1);
        AggregateFeatures(ExecutionOperatingUnitType::HASHJOIN_BUILD, key_size, num_key, c_plan, 1, scale);
    }

    // Probe
    if (translator->IsRightPipeline(*current_pipeline_)) {
        for (auto key : plan->GetRightHashKeys()) {
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));
        }

        // Record features using the row/cardinality of right plan which is probe
        auto *c_plan = plan->GetChild(1);
        RecordArithmeticFeatures(c_plan, 1);

        auto num_key = plan->GetRightHashKeys().size();
        auto key_size = ComputeKeySize(plan->GetRightHashKeys(), &num_key);
        AggregateFeatures(ExecutionOperatingUnitType::HASHJOIN_PROBE, key_size, num_key, plan, 1, 1);

        // Computes against OutputSchema/Join predicate which will
        // use the rows/cardinalities of what the HJ plan produces
        VisitAbstractJoinPlanNode(plan);
        RecordArithmeticFeatures(plan, 1);
    }
}

void OperatingUnitRecorder::Visit(const planner::NestedLoopJoinPlanNode *plan) {
    // TODO((wz2): Need an outer loop est number rows/invocation times
    [[maybe_unused]] auto *c_plan = plan->GetChild(1);
    size_t                 outer_num = 1;
    VisitAbstractJoinPlanNode(plan);
    if (plan->GetJoinPredicate() != nullptr) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(plan->GetJoinPredicate());
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));
    }
    RecordArithmeticFeatures(c_plan, outer_num);
}

void OperatingUnitRecorder::Visit(const planner::IndexJoinPlanNode *plan) {
    // Scale by num_rows - 1 of the child
    [[maybe_unused]] auto *c_plan = plan->GetChild(0);

    // Get features
    std::unordered_set<catalog::indexkeycol_oid_t> cols;
    for (auto &pair : plan->GetLoIndexColumns()) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        cols.insert(pair.first);
    }

    for (auto &pair : plan->GetHiIndexColumns()) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        cols.insert(pair.first);
    }

    // Above operations are done once per tuple in the child
    RecordArithmeticFeatures(c_plan, 1);

    // Computes against OutputSchema which will use the rows that join produces
    VisitAbstractJoinPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // Vector of indexkeycol_oid_t columns
    std::vector<catalog::indexkeycol_oid_t> col_vec;
    col_vec.reserve(cols.size());
    for (auto &col : cols) {
        col_vec.emplace_back(col);
    }

    // IndexScan output number of rows is the "cumulative scan size"
    size_t num_keys = col_vec.size();
    size_t key_size = ComputeKeySize(plan->GetIndexOid(), col_vec, &num_keys);
    AggregateFeatures(selfdriving::ExecutionOperatingUnitType::IDX_SCAN, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::InsertPlanNode *plan) {
    if (plan->GetChildrenSize() == 0) {
        // INSERT without a SELECT
        for (size_t idx = 0; idx < plan->GetBulkInsertCount(); idx++) {
            for (auto &col : plan->GetValues(idx)) {
                auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(col);
                arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                                 std::make_move_iterator(features.begin()),
                                                 std::make_move_iterator(features.end()));
            }
        }

        // Record features
        VisitAbstractPlanNode(plan);
        RecordArithmeticFeatures(plan, 1);

        // Record the Insert
        auto num_key = plan->GetParameterInfo().size();
        auto key_size = ComputeKeySize(plan->GetTableOid(), plan->GetParameterInfo(), &num_key);
        AggregateFeatures(plan_feature_type_, key_size, num_key, plan, 1, 1);
    } else {
        // INSERT with a SELECT
        auto *c_plan = plan->GetChild(0);
        NOISEPAGE_ASSERT(c_plan->GetOutputSchema() != nullptr, "Child must have OutputSchema");

        auto num_key = c_plan->GetOutputSchema()->GetColumns().size();
        auto size = ComputeKeySizeOutputSchema(c_plan, &num_key);
        AggregateFeatures(plan_feature_type_, size, num_key, plan, 1, 1);
    }

    if (!plan->GetIndexOids().empty()) {
        RecordIndexOperations<planner::InsertPlanNode>(plan->GetIndexOids(), plan->GetTableOid());
    }
}

void OperatingUnitRecorder::Visit(const planner::UpdatePlanNode *plan) {
    std::vector<catalog::col_oid_t> cols;
    for (auto &clause : plan->GetSetClauses()) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(clause.second);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        cols.emplace_back(clause.first);
    }

    // Record features
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    if (plan->GetIndexOids().empty()) {
        // Record an "in-place" update
        auto num_key = cols.size();
        auto key_size = ComputeKeySize(plan->GetTableOid(), cols, &num_key);
        AggregateFeatures(plan_feature_type_, key_size, num_key, plan, 1, 1);
    } else {
        // Record an TableDelete followed by TableInsert
        auto &schema = accessor_->GetSchema(plan->GetTableOid());
        auto  num_cols = schema.GetColumns().size();
        auto  key_size = ComputeKeySize(plan->GetTableOid(), &num_cols);
        AggregateFeatures(selfdriving::ExecutionOperatingUnitType::INSERT, key_size, num_cols, plan, 1, 1);
        AggregateFeatures(selfdriving::ExecutionOperatingUnitType::DELETE, key_size, num_cols, plan, 1, 1);

        RecordIndexOperations<planner::UpdatePlanNode>(plan->GetIndexOids(), plan->GetTableOid());
    }
}

void OperatingUnitRecorder::Visit(const planner::DeletePlanNode *plan) {
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    auto &schema = accessor_->GetSchema(plan->GetTableOid());
    auto  num_cols = schema.GetColumns().size();
    auto  key_size = ComputeKeySize(plan->GetTableOid(), &num_cols);
    AggregateFeatures(plan_feature_type_, key_size, num_cols, plan, 1, 1);

    if (!plan->GetIndexOids().empty()) {
        RecordIndexOperations<planner::DeletePlanNode>(plan->GetIndexOids(), plan->GetTableOid());
    }
}

void OperatingUnitRecorder::Visit(const planner::CSVScanPlanNode *plan) {
    VisitAbstractScanPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    auto   num_keys = plan->GetValueTypes().size();
    size_t key_size = 0;
    for (auto type : plan->GetValueTypes()) {
        key_size += execution::sql::GetSqlTypeIdSize(type);
    }

    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::LimitPlanNode *plan) {
    // Limit plan does not have its own operator and we don't model Limit with our operating units. So we skip.
}

void OperatingUnitRecorder::Visit(const planner::CteScanPlanNode *plan) {
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // Copy outwards
    auto num_keys = plan->GetOutputSchema()->GetColumns().size();
    auto key_size = 0;
    if (num_keys > 0) {
        key_size = ComputeKeySizeOutputSchema(plan, &num_keys);
    }
    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::OrderByPlanNode *plan) {
    auto translator = current_translator_.CastTo<execution::compiler::SortTranslator>();

    if (translator->IsBuildPipeline(*current_pipeline_)) {
        // SORT_BUILD will operate on sort keys
        std::vector<common::ManagedPointer<parser::AbstractExpression>> keys;
        for (auto key : plan->GetSortKeys()) {
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key.first);
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));

            keys.emplace_back(key.first);
        }

        // Get Struct and compute memory scaling factor
        auto num_key = keys.size();
        auto key_size = ComputeKeySize(keys, &num_key);
        auto scale = ComputeMemoryScaleFactor(translator->GetStructDecl(), 0, key_size, 0);

        // Sort build sizes/operations are based on the input (from child)
        const auto *c_plan = plan->GetChild(0);
        RecordArithmeticFeatures(c_plan, 1);
        ExecutionOperatingUnitType ou_type;
        if (plan->HasLimit()) {
            ou_type = ExecutionOperatingUnitType::SORT_TOPK_BUILD;
        } else {
            ou_type = ExecutionOperatingUnitType::SORT_BUILD;
        }
        AggregateFeatures(ou_type, key_size, num_key, c_plan, 1, scale);
    } else if (translator->IsScanPipeline(*current_pipeline_)) {
        // SORT_ITERATE will do any output computations
        VisitAbstractPlanNode(plan);
        RecordArithmeticFeatures(plan, 1);

        // Copy outwards
        auto num_keys = plan->GetOutputSchema()->GetColumns().size();
        auto key_size = ComputeKeySizeOutputSchema(plan, &num_keys);
        AggregateFeatures(ExecutionOperatingUnitType::SORT_ITERATE, key_size, num_keys, plan, 1, 1);
    }
}

void OperatingUnitRecorder::Visit(const planner::ProjectionPlanNode *plan) {
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);
}

void OperatingUnitRecorder::Visit(const planner::AggregatePlanNode *plan) {
    if (plan->IsStaticAggregation()) {
        auto translator = current_translator_.CastTo<execution::compiler::StaticAggregationTranslator>();
        RecordAggregateTranslator(translator, plan);
    } else {
        auto translator = current_translator_.CastTo<execution::compiler::HashAggregationTranslator>();
        RecordAggregateTranslator(translator, plan);
    }
}

template <typename Translator>
void OperatingUnitRecorder::RecordAggregateTranslator(common::ManagedPointer<Translator> translator,
                                                      const planner::AggregatePlanNode  *plan) {
    if (translator->IsBuildPipeline(*current_pipeline_)) {
        for (auto key : plan->GetAggregateTerms()) {
            auto key_cm = common::ManagedPointer<parser::AbstractExpression>(key.Get());
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key_cm);
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));
        }

        for (auto key : plan->GetGroupByTerms()) {
            auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
            arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                             std::make_move_iterator(features.begin()),
                                             std::make_move_iterator(features.end()));
        }

        // Above computations performed for all of child
        auto *c_plan = plan->GetChild(0);
        RecordArithmeticFeatures(c_plan, 1);

        // Build with the rows of child
        size_t key_size = 0;
        size_t num_keys = 0;
        double mem_factor = 1;
        if (!plan->GetGroupByTerms().empty()) {
            num_keys = plan->GetGroupByTerms().size();
            key_size = ComputeKeySize(plan->GetGroupByTerms(), &num_keys);

            // Get Struct and compute memory scaling factor
            auto offset = sizeof(execution::sql::HashTableEntry);
            auto ref_offset = offset + sizeof(execution::sql::CountAggregate);
            mem_factor = ComputeMemoryScaleFactor(translator->GetStructDecl(), offset, key_size, ref_offset);
        } else {
            std::vector<common::ManagedPointer<parser::AbstractExpression>> keys;
            for (auto term : plan->GetAggregateTerms()) {
                if (term->IsDistinct()) {
                    keys.emplace_back(term.Get());
                }
            }

            if (!keys.empty()) {
                num_keys = keys.size();
                key_size = ComputeKeySize(keys, &num_keys);
            } else {
                // This case is typically just numerics (i.e COUNT)
                // We still record something to differentiate in the models.
                num_keys = plan->GetAggregateTerms().size();
            }
        }

        AggregateFeatures(ExecutionOperatingUnitType::AGGREGATE_BUILD, key_size, num_keys, c_plan, 1, mem_factor);
    } else if (translator->IsProducePipeline(*current_pipeline_)) {
        // AggregateTopTranslator handles any exprs/computations in the output
        VisitAbstractPlanNode(plan);

        // AggregateTopTranslator handles the having clause
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(plan->GetHavingClausePredicate());
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(),
                                         std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));

        RecordArithmeticFeatures(plan, 1);

        // Copy outwards
        auto num_keys = plan->GetOutputSchema()->GetColumns().size();
        auto key_size = ComputeKeySizeOutputSchema(plan, &num_keys);
        AggregateFeatures(ExecutionOperatingUnitType::AGGREGATE_ITERATE, key_size, num_keys, plan, 1, 1);
    }
}

auto OperatingUnitRecorder::RecordTranslators(const std::vector<execution::compiler::OperatorTranslator *> &translators)
    -> ExecutionOperatingUnitFeatureVector {
    pipeline_features_ = {};
    for (const auto &translator : translators) {
        plan_feature_type_ = translator->GetFeatureType();
        current_translator_ = common::ManagedPointer(translator);

        if (plan_feature_type_ != ExecutionOperatingUnitType::INVALID) {
            if (plan_feature_type_ == ExecutionOperatingUnitType::OUTPUT) {
                NOISEPAGE_ASSERT(translator->GetChildTranslator(), "OUTPUT should have child translator");
                auto child_type = translator->GetChildTranslator()->GetFeatureType();
                if (child_type == ExecutionOperatingUnitType::INSERT || child_type == ExecutionOperatingUnitType::UPDATE
                    || child_type == ExecutionOperatingUnitType::DELETE) {
                    // For insert/update/delete, there actually is no real output happening.
                    // So, skip the Output operator
                    continue;
                }

                auto *op = translator->GetChildTranslator()->Op();
                auto  num_keys = op->GetOutputSchema()->GetColumns().size();
                auto  key_size = ComputeKeySizeOutputSchema(op, &num_keys);
                AggregateFeatures(plan_feature_type_, key_size, num_keys, op, 1, 1);
            } else {
                translator->Op()->Accept(common::ManagedPointer<planner::PlanVisitor>(this));
                NOISEPAGE_ASSERT(arithmetic_feature_types_.empty(), "aggregate_feature_types_ should be empty");
            }
        }
    }

    // Consolidate final features
    std::vector<ExecutionOperatingUnitFeature> results{};
    results.reserve(pipeline_features_.size());
    for (auto &feature : pipeline_features_) {
        results.emplace_back(feature.second);
    }

    pipeline_features_.clear();
    return results;
}

} // namespace noisepage::selfdriving
