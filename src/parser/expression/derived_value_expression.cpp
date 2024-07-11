#include "parser/expression/derived_value_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

auto DerivedValueExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<DerivedValueExpression>(GetReturnValueType(), GetTupleIdx(), GetValueIdx());
    expr->SetMutableStateForCopy(*this);
    return expr;
}

auto DerivedValueExpression::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(tuple_idx_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(value_idx_));
    return hash;
}

auto DerivedValueExpression::operator==(const AbstractExpression &rhs) const -> bool {
    if (!AbstractExpression::operator==(rhs)) {
        return false;
    }
    auto const &other = dynamic_cast<const DerivedValueExpression &>(rhs);
    if (GetTupleIdx() != other.GetTupleIdx()) {
        return false;
    }
    return GetValueIdx() == other.GetValueIdx();
}

void DerivedValueExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

auto DerivedValueExpression::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractExpression::ToJson();
    j["tuple_idx"] = tuple_idx_;
    j["value_idx"] = value_idx_;
    return j;
}

auto DerivedValueExpression::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<AbstractExpression>> {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto                                             e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    tuple_idx_ = j.at("tuple_idx").get<int>();
    value_idx_ = j.at("value_idx").get<int>();
    return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DerivedValueExpression);

} // namespace noisepage::parser
