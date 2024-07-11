#include "planner/plannodes/nested_loop_join_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

auto NestedLoopJoinPlanNode::Builder::Build() -> std::unique_ptr<NestedLoopJoinPlanNode> {
    return std::unique_ptr<NestedLoopJoinPlanNode>(new NestedLoopJoinPlanNode(std::move(children_),
                                                                              std::move(output_schema_),
                                                                              join_type_,
                                                                              join_predicate_,
                                                                              plan_node_id_));
}

NestedLoopJoinPlanNode::NestedLoopJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>>   &&children,
                                               std::unique_ptr<OutputSchema>                      output_schema,
                                               LogicalJoinType                                    join_type,
                                               common::ManagedPointer<parser::AbstractExpression> predicate,
                                               plan_node_id_t                                     plan_node_id)
    : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate, plan_node_id) {}

auto NestedLoopJoinPlanNode::Hash() const -> common::hash_t {
    return AbstractJoinPlanNode::Hash();
}

auto NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const -> bool {
    // There is nothing else for us to do here! Go home! You're drunk!
    // Unfortunately, now there is something to do...
    return AbstractJoinPlanNode::operator==(rhs);
}

auto NestedLoopJoinPlanNode::ToJson() const -> nlohmann::json {
    return AbstractJoinPlanNode::ToJson();
}

auto NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j)
    -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
    auto                                                     e1 = AbstractJoinPlanNode::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(NestedLoopJoinPlanNode);

} // namespace noisepage::planner
