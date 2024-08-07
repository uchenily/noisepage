#include "parser/expression/operator_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

auto OperatorExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
        children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
}

auto OperatorExpression::CopyWithChildren(std::vector<std::unique_ptr<AbstractExpression>> &&children) const
    -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<OperatorExpression>(GetExpressionType(), GetReturnValueType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
}

void OperatorExpression::DeriveReturnValueType() {
    // if we are a decimal or int we should take the highest type id of both children
    // This relies on a particular order in expression_defs.h
    if (this->GetExpressionType() == ExpressionType::OPERATOR_NOT
        || this->GetExpressionType() == ExpressionType::OPERATOR_IS_NULL
        || this->GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL
        || this->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
        this->SetReturnValueType(execution::sql::SqlTypeId::Boolean);
        return;
    }
    const auto &children = this->GetChildren();
    const auto &max_type_child = std::max_element(children.begin(), children.end(), [](const auto &t1, const auto &t2) {
        return t1->GetReturnValueType() < t2->GetReturnValueType();
    });
    const auto &type = (*max_type_child)->GetReturnValueType();
    NOISEPAGE_ASSERT(type <= execution::sql::SqlTypeId::Double, "Invalid operand type in Operator Expression.");
    // TODO(Matt): What is this assertion doing? Why is order of the enum important?
    this->SetReturnValueType(type);
}

void OperatorExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(OperatorExpression);

} // namespace noisepage::parser
