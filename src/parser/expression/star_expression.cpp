#include "parser/expression/star_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

auto StarExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    auto expr = std::make_unique<StarExpression>();
    expr->SetMutableStateForCopy(*this);
    return expr;
}

void StarExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
    v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(StarExpression);

} // namespace noisepage::parser
