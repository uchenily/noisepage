#include "planner/plannodes/drop_database_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

auto DropDatabasePlanNode::Builder::Build() -> std::unique_ptr<DropDatabasePlanNode> {
    return std::unique_ptr<DropDatabasePlanNode>(
        new DropDatabasePlanNode(std::move(children_), std::move(output_schema_), database_oid_, plan_node_id_));
}

DropDatabasePlanNode::DropDatabasePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                           std::unique_ptr<OutputSchema>                    output_schema,
                                           catalog::db_oid_t                                database_oid,
                                           plan_node_id_t                                   plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id)
    , database_oid_(database_oid) {}

auto DropDatabasePlanNode::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractPlanNode::Hash();

    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

    return hash;
}

auto DropDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const -> bool {
    if (!AbstractPlanNode::operator==(rhs)) {
        return false;
    }

    auto &other = dynamic_cast<const DropDatabasePlanNode &>(rhs);

    return database_oid_ == other.database_oid_;
}

auto DropDatabasePlanNode::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractPlanNode::ToJson();
    j["database_oid"] = database_oid_;
    return j;
}

auto DropDatabasePlanNode::FromJson(const nlohmann::json &j)
    -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
    auto                                                     e1 = AbstractPlanNode::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
    return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(DropDatabasePlanNode);

} // namespace noisepage::planner
