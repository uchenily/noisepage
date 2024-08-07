#include "parser/expression/function_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

auto FunctionExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
        children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
}

auto FunctionExpression::CopyWithChildren(std::vector<std::unique_ptr<AbstractExpression>> &&children) const
    -> std::unique_ptr<AbstractExpression> {
    std::string func_name = GetFuncName();
    auto expr = std::make_unique<FunctionExpression>(std::move(func_name), GetReturnValueType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    expr->SetProcOid(GetProcOid());
    return expr;
}

auto FunctionExpression::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractExpression::ToJson();
    j["func_name"] = func_name_;
    return j;
}

auto FunctionExpression::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<AbstractExpression>> {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto                                             e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    func_name_ = j.at("func_name").get<std::string>();
    return exprs;
}

void FunctionExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

auto FunctionExpression::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(func_name_));
    return hash;
}

DEFINE_JSON_BODY_DECLARATIONS(FunctionExpression);

} // namespace noisepage::parser
