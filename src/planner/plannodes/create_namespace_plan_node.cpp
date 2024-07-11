#include "planner/plannodes/create_namespace_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "parser/parser_defs.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

auto CreateNamespacePlanNode::Builder::Build() -> std::unique_ptr<CreateNamespacePlanNode> {
    return std::unique_ptr<CreateNamespacePlanNode>(new CreateNamespacePlanNode(std::move(children_),
                                                                                std::move(output_schema_),
                                                                                std::move(namespace_name_),
                                                                                plan_node_id_));
}

CreateNamespacePlanNode::CreateNamespacePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                                 std::unique_ptr<OutputSchema>                    output_schema,
                                                 std::string                                      namespace_name,
                                                 plan_node_id_t                                   plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id)
    , namespace_name_(std::move(namespace_name)) {}

auto CreateNamespacePlanNode::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractPlanNode::Hash();

    // Hash namespace_name
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));

    return hash;
}

auto CreateNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const -> bool {
    if (!AbstractPlanNode::operator==(rhs)) {
        return false;
    }

    auto &other = dynamic_cast<const CreateNamespacePlanNode &>(rhs);

    // Schema name
    return namespace_name_ == other.namespace_name_;
}

auto CreateNamespacePlanNode::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractPlanNode::ToJson();
    j["namespace_name"] = namespace_name_;
    return j;
}

auto CreateNamespacePlanNode::FromJson(const nlohmann::json &j)
    -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
    auto                                                     e1 = AbstractPlanNode::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    namespace_name_ = j.at("namespace_name").get<std::string>();
    return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(CreateNamespacePlanNode);

} // namespace noisepage::planner
