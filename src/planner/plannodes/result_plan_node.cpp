#include "planner/plannodes/result_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

auto ResultPlanNode::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractPlanNode::Hash();

    // Expression
    hash = common::HashUtil::CombineHashes(hash, expr_->Hash());

    return hash;
}

auto ResultPlanNode::operator==(const AbstractPlanNode &rhs) const -> bool {
    if (!AbstractPlanNode::operator==(rhs)) {
        return false;
    }

    auto &other = dynamic_cast<const ResultPlanNode &>(rhs);

    // Expression
    if ((expr_ != nullptr && other.expr_ == nullptr) || (expr_ == nullptr && other.expr_ != nullptr)) {
        return false;
    }
    if (expr_ != nullptr && *expr_ != *other.expr_) {
        return false;
    }

    return true;
}

auto ResultPlanNode::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractPlanNode::ToJson();
    j["expr"] = expr_ == nullptr ? nlohmann::json(nullptr) : expr_->ToJson();
    return j;
}

auto ResultPlanNode::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
    auto                                                     e1 = AbstractPlanNode::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    if (!j.at("expr").is_null()) {
        auto deserialized = parser::DeserializeExpression(j.at("expr"));
        expr_ = common::ManagedPointer(deserialized.result_);
        exprs.emplace_back(std::move(deserialized.result_));
        exprs.insert(exprs.end(),
                     std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                     std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
    return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ResultPlanNode);

} // namespace noisepage::planner
