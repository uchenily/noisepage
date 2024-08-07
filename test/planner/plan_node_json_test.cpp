#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "nlohmann/json.hpp"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
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
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/result_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage::planner {

class PlanNodeJsonTest : public TerrierTest {
public:
    /**
     * Constructs a dummy OutputSchema object with a single column
     * @return dummy output schema
     */
    static std::unique_ptr<OutputSchema> BuildDummyOutputSchema() {
        std::vector<OutputSchema::Column> cols;
        cols.emplace_back(OutputSchema::Column("dummy_col", execution::sql::SqlTypeId::Integer, BuildDummyPredicate()));
        return std::make_unique<OutputSchema>(std::move(cols));
    }

    /**
     * Constructs a dummy AbstractExpression predicate
     * @return dummy predicate
     */
    static std::unique_ptr<parser::AbstractExpression> BuildDummyPredicate() {
        return std::make_unique<parser::ConstantValueExpression>(execution::sql::SqlTypeId::Boolean,
                                                                 execution::sql::BoolVal(true));
    }
};

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, OutputSchemaJsonTest) {
    // Test Column serialization
    OutputSchema::Column col("col1", execution::sql::SqlTypeId::Boolean, PlanNodeJsonTest::BuildDummyPredicate());
    auto                 col_json = col.ToJson();
    EXPECT_FALSE(col_json.is_null());

    OutputSchema::Column deserialized_col;
    deserialized_col.FromJson(col_json);
    EXPECT_EQ(col, deserialized_col);

    // Test OutputSchema Serialization
    std::vector<OutputSchema::Column> cols;
    cols.emplace_back(col.Copy());
    auto output_schema = std::make_unique<OutputSchema>(std::move(cols));
    auto output_schema_json = output_schema->ToJson();
    EXPECT_FALSE(output_schema_json.is_null());

    std::unique_ptr<OutputSchema> deserialized_output_schema = std::make_unique<OutputSchema>();
    auto                          deserialized_output_res = deserialized_output_schema->FromJson(output_schema_json);
    EXPECT_EQ(*output_schema, *deserialized_output_schema);
    EXPECT_EQ(output_schema->Hash(), deserialized_output_schema->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, AggregatePlanNodeJsonTest) {
    // Construct AggregatePlanNode

    std::vector<std::unique_ptr<parser::AbstractExpression>> children;
    children.push_back(PlanNodeJsonTest::BuildDummyPredicate());
    auto agg_term = std::make_unique<parser::AggregateExpression>(parser::ExpressionType::AGGREGATE_COUNT,
                                                                  std::move(children),
                                                                  false);
    auto plan_predicate = PlanNodeJsonTest::BuildDummyPredicate();
    AggregatePlanNode::Builder builder;
    auto                       plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetAggregateStrategyType(AggregateStrategyType::HASH)
                         .SetHavingClausePredicate(common::ManagedPointer(plan_predicate))
                         .AddAggregateTerm(common::ManagedPointer(agg_term))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<AggregatePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::AGGREGATE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, AnalyzePlanNodeJsonTest) {
    // Construct AnalyzePlanNode
    AnalyzePlanNode::Builder        builder;
    std::vector<catalog::col_oid_t> col_oids = {catalog::col_oid_t(1),
                                                catalog::col_oid_t(2),
                                                catalog::col_oid_t(3),
                                                catalog::col_oid_t(4),
                                                catalog::col_oid_t(5)};
    auto                            plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetDatabaseOid(catalog::db_oid_t(1))
                         .SetTableOid(catalog::table_oid_t(2))
                         .SetColumnOIDs(std::move(col_oids))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<AnalyzePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::ANALYZE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateDatabasePlanNodeTest) {
    // Construct CreateDatabasePlanNode
    CreateDatabasePlanNode::Builder builder;
    auto                            plan_node = builder.SetDatabaseName("test_db").Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateDatabasePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_DATABASE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateFunctionPlanNodeTest) {
    // Construct CreateFunctionPlanNode
    CreateFunctionPlanNode::Builder builder;
    auto                            plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                         .SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetLanguage(parser::PLType::PL_PGSQL)
                         .SetFunctionParamNames({"i"})
                         .SetFunctionParamTypes({parser::BaseFunctionParameter::DataType::INT})
                         .SetBody({"RETURN i+1;"})
                         .SetIsReplace(true)
                         .SetFunctionName("test_func")
                         .SetReturnType(parser::BaseFunctionParameter::DataType::INT)
                         .SetParamCount(1)
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateFunctionPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_FUNC, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateIndexPlanNodeTest) {
    // Construct CreateIndexPlanNode
    CreateIndexPlanNode::Builder builder;
    auto                         plan_node = builder.SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetTableOid(catalog::table_oid_t(2))
                         .SetIndexName("test_index")
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateIndexPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_INDEX, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateNamespacePlanNodeTest) {
    // Construct CreateNamespacePlanNode
    CreateNamespacePlanNode::Builder builder;
    auto                             plan_node = builder.SetNamespaceName("test_namespace").Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateNamespacePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_NAMESPACE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateTablePlanNodeTest) {
    // PRIMARY KEY
    auto get_pk_info = []() {
        PrimaryKeyInfo pk = {.primary_key_cols_ = {"a"}, .constraint_name_ = "pk_a"};
        return pk;
    };

    // FOREIGN KEY
    auto get_fk_info = []() {
        std::vector<ForeignKeyInfo> checks;
        ForeignKeyInfo              fk = {.foreign_key_sources_ = {"b"},
                                          .foreign_key_sinks_ = {"b"},
                                          .sink_table_name_ = {"tbl2"},
                                          .constraint_name_ = "fk_b",
                                          .upd_action_ = parser::FKConstrActionType::CASCADE,
                                          .del_action_ = parser::FKConstrActionType::CASCADE};
        checks.emplace_back(fk);
        return checks;
    };

    // UNIQUE CONSTRAINT
    auto get_unique_info = []() {
        std::vector<UniqueInfo> checks;
        UniqueInfo              uk = {
                         .unique_cols_ = {"u_a", "u_b"},
                         .constraint_name_ = "uk_a_b"
        };
        checks.emplace_back(uk);
        return checks;
    };

    // CHECK CONSTRAINT
    auto get_check_info = []() {
        parser::ConstantValueExpression val(execution::sql::SqlTypeId::Integer, execution::sql::Integer(1));
        std::vector<CheckInfo>          checks;
        std::vector<std::string>        cks = {"ck_a"};
        checks.emplace_back(cks, "ck_a", parser::ExpressionType::COMPARE_GREATER_THAN, std::move(val));
        return checks;
    };

    // Columns
    auto get_schema = []() {
        std::vector<catalog::Schema::Column> columns
            = {catalog::Schema::Column("a",
                                       execution::sql::SqlTypeId::Integer,
                                       false,
                                       parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer)),
               catalog::Schema::Column("u_a",
                                       execution::sql::SqlTypeId::Double,
                                       false,
                                       parser::ConstantValueExpression(execution::sql::SqlTypeId::Double)),
               catalog::Schema::Column("u_b",
                                       execution::sql::SqlTypeId::Date,
                                       true,
                                       parser::ConstantValueExpression(execution::sql::SqlTypeId::Date))};
        StorageTestUtil::ForceOid(&(columns[0]), catalog::col_oid_t(1));
        StorageTestUtil::ForceOid(&(columns[1]), catalog::col_oid_t(2));
        StorageTestUtil::ForceOid(&(columns[2]), catalog::col_oid_t(3));
        return std::make_unique<catalog::Schema>(columns);
    };

    // Construct CreateTablePlanNode (1 with PK and 1 without PK)
    CreateTablePlanNode::Builder builder;
    auto                         pk_plan_node = builder.SetNamespaceOid(catalog::namespace_oid_t(2))
                            .SetTableName("test_tbl")
                            .SetTableSchema(get_schema())
                            .SetHasPrimaryKey(true)
                            .SetPrimaryKey(get_pk_info())
                            .SetForeignKeys(get_fk_info())
                            .SetUniqueConstraints(get_unique_info())
                            .SetCheckConstraints(get_check_info())
                            .Build();

    auto no_pk_plan_node = builder.SetNamespaceOid(catalog::namespace_oid_t(2))
                               .SetTableName("test_tbl")
                               .SetTableSchema(get_schema())
                               .SetHasPrimaryKey(false)
                               .SetPrimaryKey(get_pk_info())
                               .SetForeignKeys(get_fk_info())
                               .SetUniqueConstraints(get_unique_info())
                               .SetCheckConstraints(get_check_info())
                               .Build();
    EXPECT_NE(*pk_plan_node, *no_pk_plan_node);
    EXPECT_NE(pk_plan_node->Hash(), no_pk_plan_node->Hash());

    // Serialize to Json
    auto pk_json = pk_plan_node->ToJson();
    auto no_pk_json = no_pk_plan_node->ToJson();
    EXPECT_FALSE(pk_json.is_null());
    EXPECT_FALSE(no_pk_json.is_null());

    // Deserialize plan node
    auto deserialized_pk = DeserializePlanNode(pk_json);
    auto deserialized_no_pk = DeserializePlanNode(no_pk_json);
    auto deserialized_pk_plan = common::ManagedPointer(deserialized_pk.result_).CastTo<CreateTablePlanNode>();
    auto deserialized_no_pk_plan = common::ManagedPointer(deserialized_no_pk.result_).CastTo<CreateTablePlanNode>();
    EXPECT_TRUE(deserialized_pk_plan != nullptr);
    EXPECT_TRUE(deserialized_no_pk_plan != nullptr);

    EXPECT_EQ(PlanNodeType::CREATE_TABLE, deserialized_pk_plan->GetPlanNodeType());
    EXPECT_EQ(PlanNodeType::CREATE_TABLE, deserialized_no_pk_plan->GetPlanNodeType());

    EXPECT_NE(*deserialized_pk_plan, *deserialized_no_pk_plan);
    EXPECT_NE(deserialized_pk_plan->Hash(), deserialized_no_pk_plan->Hash());

    // PRIMARY KEY
    EXPECT_EQ(*pk_plan_node, *deserialized_pk_plan);
    EXPECT_EQ(pk_plan_node->Hash(), deserialized_pk_plan->Hash());

    // NO PRIMARY KEY
    EXPECT_EQ(*no_pk_plan_node, *deserialized_no_pk_plan);
    EXPECT_EQ(no_pk_plan_node->Hash(), deserialized_no_pk_plan->Hash());

    // Foreign Key Constraints
    EXPECT_EQ(deserialized_pk_plan->GetForeignKeys().size(), 1);
    EXPECT_EQ(deserialized_pk_plan->GetForeignKeys()[0], get_fk_info()[0]);
    EXPECT_EQ(deserialized_pk_plan->GetForeignKeys()[0].Hash(), get_fk_info()[0].Hash());

    // Unique Constraints
    EXPECT_EQ(deserialized_pk_plan->GetUniqueConstraints().size(), 1);
    EXPECT_EQ(deserialized_pk_plan->GetUniqueConstraints()[0], get_unique_info()[0]);
    EXPECT_EQ(deserialized_pk_plan->GetUniqueConstraints()[0].Hash(), get_unique_info()[0].Hash());

    // Check Constraints
    EXPECT_EQ(deserialized_pk_plan->GetCheckConstraints().size(), 1);
    EXPECT_EQ(deserialized_pk_plan->GetCheckConstraints()[0], get_check_info()[0]);
    EXPECT_EQ(deserialized_pk_plan->GetCheckConstraints()[0].Hash(), get_check_info()[0].Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateTriggerPlanNodeTest) {
    // Construct CreateTriggerPlanNode
    auto                           when_pred = PlanNodeJsonTest::BuildDummyPredicate();
    CreateTriggerPlanNode::Builder builder;
    auto                           plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2))
                         .SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetTableOid(catalog::table_oid_t(3))
                         .SetTriggerName("test_trigger")
                         .SetTriggerFuncnames({"test_trigger_func"})
                         .SetTriggerArgs({"a", "b"})
                         .SetTriggerColumns({catalog::col_oid_t(0), catalog::col_oid_t(1)})
                         .SetTriggerWhen(common::ManagedPointer(when_pred))
                         .SetTriggerType(23)
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateTriggerPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_TRIGGER, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateViewPlanNodeTest) {
    // Construct CreateViewPlanNode
    CreateViewPlanNode::Builder              builder;
    std::unique_ptr<parser::SelectStatement> select_stmt = std::make_unique<parser::SelectStatement>();
    auto                                     plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2))
                         .SetNamespaceOid(catalog::namespace_oid_t(3))
                         .SetViewName("test_view")
                         .SetViewQuery(std::move(select_stmt))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CreateViewPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CREATE_VIEW, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CSVScanPlanNodeTest) {
    // Construct CSVScanPlanNode
    CSVScanPlanNode::Builder builder;
    auto plan_node = builder.SetFileName("/dev/null").SetDelimiter(',').SetQuote('\'').SetEscape('`').Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<CSVScanPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::CSVSCAN, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DeletePlanNodeTest) {
    // Construct DeletePlanNode
    auto                    delete_pred = PlanNodeJsonTest::BuildDummyPredicate();
    DeletePlanNode::Builder builder;
    auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1)).SetTableOid(catalog::table_oid_t(2)).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DeletePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DELETE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropDatabasePlanNodeTest) {
    // Construct DropDatabasePlanNode
    DropDatabasePlanNode::Builder builder;
    auto                          plan_node = builder.SetDatabaseOid(catalog::db_oid_t(7)).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropDatabasePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_DATABASE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());

    // Sanity check to make sure that it actually fails if the plan nodes are truly different
    DropDatabasePlanNode::Builder builder2;
    auto                          plan_node2 = builder2.SetDatabaseOid(catalog::db_oid_t(9999)).Build();
    auto                          json2 = plan_node2->ToJson();
    auto                          deserialized2 = DeserializePlanNode(json2);
    auto deserialized_plan2 = common::ManagedPointer(deserialized2.result_).CastTo<DropDatabasePlanNode>();
    EXPECT_NE(*plan_node, *deserialized_plan2);
    EXPECT_NE(*deserialized_plan, *deserialized_plan2);
    EXPECT_NE(plan_node->Hash(), deserialized_plan2->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropIndexPlanNodeTest) {
    // Construct DropIndexPlanNode
    DropIndexPlanNode::Builder builder;
    auto                       plan_node = builder.SetIndexOid(catalog::index_oid_t(8)).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropIndexPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_INDEX, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropNamespacePlanNodeTest) {
    // Construct DropNamespacePlanNode
    DropNamespacePlanNode::Builder builder;
    auto                           plan_node = builder.SetNamespaceOid(catalog::namespace_oid_t(9)).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropNamespacePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_NAMESPACE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropTablePlanNodeTest) {
    // Construct DropTablePlanNode
    DropTablePlanNode::Builder builder;
    auto                       plan_node = builder.SetTableOid(catalog::table_oid_t(10)).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropTablePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_TABLE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropTriggerPlanNodeTest) {
    // Construct DropTriggerPlanNode
    DropTriggerPlanNode::Builder builder;
    auto                         plan_node = builder.SetDatabaseOid(catalog::db_oid_t(10))
                         .SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetTriggerOid(catalog::trigger_oid_t(11))
                         .SetIfExist(true)
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropTriggerPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_TRIGGER, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropViewPlanNodeTest) {
    // Construct DropViewPlanNode
    DropViewPlanNode::Builder builder;
    auto                      plan_node
        = builder.SetDatabaseOid(catalog::db_oid_t(11)).SetViewOid(catalog::view_oid_t(12)).SetIfExist(true).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<DropViewPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::DROP_VIEW, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ExportExternalFilePlanNodeJsonTest) {
    // Construct ExportExternalFilePlanNode
    ExportExternalFilePlanNode::Builder builder;
    auto                                plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetFileName("test_file")
                         .SetDelimiter(',')
                         .SetEscape('"')
                         .SetQuote('"')
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<ExportExternalFilePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::EXPORT_EXTERNAL_FILE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, HashJoinPlanNodeJoinTest) {
    // Construct HashJoinPlanNode
    auto left_hash_key = std::make_unique<parser::ColumnValueExpression>(parser::AliasType("table1"), "col1");
    auto right_hash_key = std::make_unique<parser::ColumnValueExpression>(parser::AliasType("table2"), "col2");
    auto join_pred = PlanNodeJsonTest::BuildDummyPredicate();
    HashJoinPlanNode::Builder builder;
    auto                      plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetJoinType(LogicalJoinType::INNER)
                         .SetJoinPredicate(common::ManagedPointer(join_pred))
                         .AddLeftHashKey(common::ManagedPointer(left_hash_key).CastTo<parser::AbstractExpression>())
                         .AddRightHashKey(common::ManagedPointer(right_hash_key).CastTo<parser::AbstractExpression>())
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<HashJoinPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::HASHJOIN, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, IndexScanPlanNodeJsonTest) {
    // Construct IndexScanPlanNode
    auto                       scan_pred = PlanNodeJsonTest::BuildDummyPredicate();
    IndexScanPlanNode::Builder builder;
    auto                       plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetScanPredicate(common::ManagedPointer(scan_pred))
                         .SetIsForUpdateFlag(false)
                         .SetDatabaseOid(catalog::db_oid_t(0))
                         .SetIndexOid(catalog::index_oid_t(0))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<IndexScanPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::INDEXSCAN, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, InsertPlanNodeJsonTest) {
    // Construct InsertPlanNode
    std::vector<const parser::AbstractExpression *> free_exprs;

    // Values Generator
    auto get_values = [&](int offset, int num_cols) {
        std::vector<common::ManagedPointer<parser::AbstractExpression>> tuple;

        auto ptr
            = new parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer, execution::sql::Integer(offset));
        free_exprs.push_back(ptr);
        tuple.emplace_back(ptr);
        for (; num_cols - 1 > 0; num_cols--) {
            auto cve = new parser::ConstantValueExpression(execution::sql::SqlTypeId::Boolean,
                                                           execution::sql::BoolVal(true));
            free_exprs.push_back(cve);
            tuple.emplace_back(cve);
        }
        return tuple;
    };

    InsertPlanNode::Builder builder;
    auto                    plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetDatabaseOid(catalog::db_oid_t(0))
                         .SetTableOid(catalog::table_oid_t(1))
                         .AddValues(get_values(0, 2))
                         .AddValues(get_values(1, 2))
                         .AddParameterInfo(catalog::col_oid_t(0))
                         .AddParameterInfo(catalog::col_oid_t(1))
                         .SetIndexOids({catalog::index_oid_t{0}, catalog::index_oid_t{1}})
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<InsertPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::INSERT, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());

    // Make sure that we are checking the ParameterInfo map correctly!
    InsertPlanNode::Builder builder2;
    auto                    plan_node2 = builder2.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                          .SetDatabaseOid(catalog::db_oid_t(0))
                          .SetTableOid(catalog::table_oid_t(1))
                          .AddValues(get_values(0, 3))
                          .AddValues(get_values(1, 3))
                          .AddParameterInfo(catalog::col_oid_t(0))
                          .AddParameterInfo(catalog::col_oid_t(1))
                          .AddParameterInfo(catalog::col_oid_t(2))
                          .Build();
    auto json2 = plan_node2->ToJson();
    EXPECT_FALSE(json2.is_null());
    auto deserialized2 = DeserializePlanNode(json2);
    auto deserialized_plan2 = common::ManagedPointer(deserialized2.result_).CastTo<InsertPlanNode>();
    EXPECT_TRUE(deserialized_plan2 != nullptr);
    EXPECT_EQ(PlanNodeType::INSERT, deserialized_plan2->GetPlanNodeType());
    EXPECT_NE(*plan_node, *deserialized_plan2);
    EXPECT_NE(plan_node->Hash(), deserialized_plan2->Hash());

    for (auto *ptr : free_exprs)
        delete ptr;
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, LimitPlanNodeJsonTest) {
    // Construct LimitPlanNode
    LimitPlanNode::Builder builder;
    auto                   plan_node
        = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).SetLimit(10).SetOffset(10).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<LimitPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::LIMIT, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, NestedLoopJoinPlanNodeJoinTest) {
    // Construct NestedLoopJoinPlanNode
    auto                            join_pred = PlanNodeJsonTest::BuildDummyPredicate();
    NestedLoopJoinPlanNode::Builder builder;
    auto                            plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetJoinType(LogicalJoinType::INNER)
                         .SetJoinPredicate(common::ManagedPointer(join_pred))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<NestedLoopJoinPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::NESTLOOP, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, OrderByPlanNodeJsonTest) {
    // Construct OrderByPlanNode
    OrderByPlanNode::Builder    builder;
    parser::AbstractExpression *sortkey1 = new parser::DerivedValueExpression(execution::sql::SqlTypeId::Integer, 0, 0);
    parser::AbstractExpression *sortkey2 = new parser::DerivedValueExpression(execution::sql::SqlTypeId::Integer, 0, 1);

    auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .AddSortKey(common::ManagedPointer(sortkey1), optimizer::OrderByOrderingType::ASC)
                         .AddSortKey(common::ManagedPointer(sortkey2), optimizer::OrderByOrderingType::DESC)
                         .SetLimit(10)
                         .SetOffset(10)
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<OrderByPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::ORDERBY, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());

    delete sortkey1;
    delete sortkey2;
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ProjectionPlanNodeJsonTest) {
    // Construct ProjectionPlanNode
    ProjectionPlanNode::Builder builder;
    auto                        plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<ProjectionPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::PROJECTION, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ResultPlanNodeJsonTest) {
    // Construct ResultPlanNode
    auto                    expr = PlanNodeJsonTest::BuildDummyPredicate();
    ResultPlanNode::Builder builder;
    auto                    plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetExpr(common::ManagedPointer(expr))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<ResultPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::RESULT, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, SeqScanPlanNodeJsonTest) {
    // Construct SeqScanPlanNode
    auto                     scan_pred = PlanNodeJsonTest::BuildDummyPredicate();
    SeqScanPlanNode::Builder builder;
    auto                     plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetScanPredicate(common::ManagedPointer(scan_pred))
                         .SetIsForUpdateFlag(false)
                         .SetDatabaseOid(catalog::db_oid_t(0))
                         .SetTableOid(catalog::table_oid_t(0))
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<SeqScanPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::SEQSCAN, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, SetOpPlanNodeJsonTest) {
    // Construct SetOpPlanNode
    SetOpPlanNode::Builder builder;
    auto                   plan_node
        = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).SetSetOp(SetOpType::INTERSECT).Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<SetOpPlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::SETOP, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, UpdatePlanNodeJsonTest) {
    UpdatePlanNode::Builder builder;
    auto                    plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                         .SetDatabaseOid(catalog::db_oid_t(1000))
                         .SetTableOid(catalog::table_oid_t(200))
                         .SetUpdatePrimaryKey(true)
                         .Build();

    // Serialize to Json
    auto json = plan_node->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize plan node
    auto deserialized = DeserializePlanNode(json);
    auto deserialized_plan = common::ManagedPointer(deserialized.result_).CastTo<UpdatePlanNode>();
    EXPECT_TRUE(deserialized_plan != nullptr);
    EXPECT_EQ(PlanNodeType::UPDATE, deserialized_plan->GetPlanNodeType());
    EXPECT_EQ(*plan_node, *deserialized_plan);
    EXPECT_EQ(plan_node->Hash(), deserialized_plan->Hash());
}

} // namespace noisepage::planner
