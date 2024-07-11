#include "parser/expression/parameter_value_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

auto ParameterValueExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<ParameterValueExpression>(GetValueIdx());
    expr->SetMutableStateForCopy(*this);
    return expr;
}

auto ParameterValueExpression::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractExpression::ToJson();
    j["value_idx"] = value_idx_;
    return j;
}

auto ParameterValueExpression::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<AbstractExpression>> {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto                                             e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    value_idx_ = j.at("value_idx").get<uint32_t>();
    return exprs;
}

void ParameterValueExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

auto ParameterValueExpression::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(value_idx_));
    return hash;
}

DEFINE_JSON_BODY_DECLARATIONS(ParameterValueExpression);

} // namespace noisepage::parser
