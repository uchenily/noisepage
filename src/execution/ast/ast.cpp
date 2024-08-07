#include "execution/ast/ast.h"

#include "execution/ast/type.h"

namespace noisepage::execution::ast {

// ---------------------------------------------------------
// Function Declaration
// ---------------------------------------------------------

FunctionDecl::FunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *func)
    : Decl(Kind::FunctionDecl, pos, name, func->TypeRepr())
    , func_(func) {}

// ---------------------------------------------------------
// Structure Declaration
// ---------------------------------------------------------

StructDecl::StructDecl(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr)
    : Decl(Kind::StructDecl, pos, name, type_repr) {}

auto StructDecl::NumFields() const -> uint32_t {
    const auto &fields = TypeRepr()->As<ast::StructTypeRepr>()->Fields();
    return fields.size();
}

auto StructDecl::GetFieldAt(uint32_t field_idx) const -> ast::FieldDecl * {
    return TypeRepr()->As<ast::StructTypeRepr>()->GetFieldAt(field_idx);
}

// ---------------------------------------------------------
// Expression Statement
// ---------------------------------------------------------

ExpressionStmt::ExpressionStmt(Expr *expr)
    : Stmt(Kind::ExpressionStmt, expr->Position())
    , expr_(expr) {}

// ---------------------------------------------------------
// Expression
// ---------------------------------------------------------

auto Expr::IsNilLiteral() const -> bool {
    if (auto *lit_expr = SafeAs<ast::LitExpr>()) {
        return lit_expr->GetLiteralKind() == ast::LitExpr::LitKind::Nil;
    }
    return false;
}

auto Expr::IsBoolLiteral() const -> bool {
    if (auto *lit_expr = SafeAs<ast::LitExpr>()) {
        return lit_expr->GetLiteralKind() == ast::LitExpr::LitKind::Boolean;
    }
    return false;
}

auto Expr::IsStringLiteral() const -> bool {
    if (auto *lit_expr = SafeAs<ast::LitExpr>()) {
        return lit_expr->GetLiteralKind() == ast::LitExpr::LitKind::String;
    }
    return false;
}

auto Expr::IsIntegerLiteral() const -> bool {
    if (auto *lit_expr = SafeAs<ast::LitExpr>()) {
        return lit_expr->GetLiteralKind() == ast::LitExpr::LitKind::Int;
    }
    return false;
}

// ---------------------------------------------------------
// Comparison Expression
// ---------------------------------------------------------

namespace {

    // Catches: nil [ '==' | '!=' ] expr
    auto MatchIsLiteralCompareNil(Expr *left, parsing::Token::Type op, Expr *right, Expr **result) -> bool {
        if (left->IsNilLiteral() && parsing::Token::IsCompareOp(op)) {
            *result = right;
            return true;
        }
        return false;
    }

} // namespace

auto ComparisonOpExpr::IsLiteralCompareNil(Expr **result) const -> bool {
    return MatchIsLiteralCompareNil(left_, op_, right_, result) || MatchIsLiteralCompareNil(right_, op_, left_, result);
}

// ---------------------------------------------------------
// Function Literal Expressions
// ---------------------------------------------------------

FunctionLitExpr::FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body)
    : Expr(Kind::FunctionLitExpr, type_repr->Position())
    , type_repr_(type_repr)
    , body_(body) {}

// ---------------------------------------------------------
// Call Expression
// ---------------------------------------------------------

auto CallExpr::GetFuncName() const -> Identifier {
    return func_->As<IdentifierExpr>()->Name();
}

// ---------------------------------------------------------
// Index Expressions
// ---------------------------------------------------------

auto IndexExpr::IsArrayAccess() const -> bool {
    NOISEPAGE_ASSERT(Object() != nullptr, "Object cannot be NULL");
    NOISEPAGE_ASSERT(Object() != nullptr, "Cannot determine object type before type checking!");
    return Object()->GetType()->IsArrayType();
}

auto IndexExpr::IsMapAccess() const -> bool {
    NOISEPAGE_ASSERT(Object() != nullptr, "Object cannot be NULL");
    NOISEPAGE_ASSERT(Object() != nullptr, "Cannot determine object type before type checking!");
    return Object()->GetType()->IsMapType();
}

// ---------------------------------------------------------
// Member expression
// ---------------------------------------------------------

auto MemberExpr::IsSugaredArrow() const -> bool {
    NOISEPAGE_ASSERT(Object()->GetType() != nullptr, "Cannot determine sugared-arrow before type checking!");
    return Object()->GetType()->IsPointerType();
}

// ---------------------------------------------------------
// Statement
// ---------------------------------------------------------

auto Stmt::IsTerminating(Stmt *stmt) -> bool {
    switch (stmt->GetKind()) {
    case AstNode::Kind::BlockStmt: {
        return IsTerminating(stmt->As<BlockStmt>()->Statements().back());
    }
    case AstNode::Kind::IfStmt: {
        auto *if_stmt = stmt->As<IfStmt>();
        return (if_stmt->HasElseStmt() && (IsTerminating(if_stmt->ThenStmt()) && IsTerminating(if_stmt->ElseStmt())));
    }
    case AstNode::Kind::ReturnStmt: {
        return true;
    }
    default: {
        return false;
    }
    }
}

auto CastKindToString(const CastKind cast_kind) -> std::string {
    switch (cast_kind) {
    case CastKind::IntToSqlInt:
        return "IntToSqlInt";
    case CastKind::IntToSqlDecimal:
        return "IntToSqlDecimal";
    case CastKind::SqlBoolToBool:
        return "SqlBoolToBool";
    case CastKind::BoolToSqlBool:
        return "BoolToSqlBool";
    case CastKind::IntegralCast:
        return "IntegralCast";
    case CastKind::IntToFloat:
        return "IntToFloat";
    case CastKind::FloatToInt:
        return "FloatToInt";
    case CastKind::BitCast:
        return "BitCast";
    case CastKind::FloatToSqlReal:
        return "FloatToSqlReal";
    case CastKind::SqlIntToSqlReal:
        return "SqlIntToSqlReal";
    default:
        UNREACHABLE("Impossible cast kind");
    }
}

} // namespace noisepage::execution::ast
