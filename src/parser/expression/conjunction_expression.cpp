#include "parser/expression/conjunction_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

auto ConjunctionExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
        children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
}

auto ConjunctionExpression::CopyWithChildren(std::vector<std::unique_ptr<AbstractExpression>> &&children) const
    -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<ConjunctionExpression>(GetExpressionType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
}

void ConjunctionExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(ConjunctionExpression);

} // namespace noisepage::parser
