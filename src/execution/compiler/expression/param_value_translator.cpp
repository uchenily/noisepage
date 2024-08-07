#include "execution/compiler/expression/param_value_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/parameter_value_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

ParamValueTranslator::ParamValueTranslator(const parser::ParameterValueExpression &expr,
                                           CompilationContext                     *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

auto ParamValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const -> ast::Expr * {
    auto        *codegen = GetCodeGen();
    auto         param_val = GetExpressionAs<noisepage::parser::ParameterValueExpression>();
    auto         param_idx = param_val.GetValueIdx();
    ast::Builtin builtin;
    switch (param_val.GetReturnValueType()) {
    case execution::sql::SqlTypeId::Boolean:
        builtin = ast::Builtin::GetParamBool;
        break;
    case execution::sql::SqlTypeId::TinyInt:
        builtin = ast::Builtin::GetParamTinyInt;
        break;
    case execution::sql::SqlTypeId::SmallInt:
        builtin = ast::Builtin::GetParamSmallInt;
        break;
    case execution::sql::SqlTypeId::Integer:
        builtin = ast::Builtin::GetParamInt;
        break;
    case execution::sql::SqlTypeId::BigInt:
        builtin = ast::Builtin::GetParamBigInt;
        break;
    case execution::sql::SqlTypeId::Double:
        builtin = ast::Builtin::GetParamDouble;
        break;
    case execution::sql::SqlTypeId::Date:
        builtin = ast::Builtin::GetParamDate;
        break;
    case execution::sql::SqlTypeId::Timestamp:
        builtin = ast::Builtin::GetParamTimestamp;
        break;
    case execution::sql::SqlTypeId::Varchar:
        builtin = ast::Builtin::GetParamString;
        break;
    default:
        UNREACHABLE("Unsupported parameter type");
    }

    return codegen->CallBuiltin(builtin, {GetExecutionContextPtr(), codegen->Const32(param_idx)});
}

} // namespace noisepage::execution::compiler
