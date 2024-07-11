#include "parser/expression/comparison_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

auto ComparisonExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
        children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
}

auto ComparisonExpression::CopyWithChildren(std::vector<std::unique_ptr<AbstractExpression>> &&children) const
    -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<ComparisonExpression>(GetExpressionType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
}

void ComparisonExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(ComparisonExpression);

} // namespace noisepage::parser
