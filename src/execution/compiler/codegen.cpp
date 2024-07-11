#include "execution/compiler/codegen.h"

#include "common/error/exception.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/executable_query_builder.h"
#include "spdlog/fmt/fmt.h"
#include "storage/index/index_defs.h"

namespace noisepage::execution::compiler {

//===----------------------------------------------------------------------===//
//
// Scopes
//
//===----------------------------------------------------------------------===//

auto CodeGen::Scope::GetFreshName(const std::string &name) -> std::string {
    // Attempt insert.
    auto insert_result = names_.insert(std::make_pair(name, 1));
    if (insert_result.second) {
        return insert_result.first->getKey().str();
    }
    // Duplicate found. Find a new version that hasn't already been declared.
    uint64_t &id = insert_result.first->getValue();
    while (true) {
        auto next_name = name + std::to_string(id++);
        if (names_.find(next_name) == names_.end()) {
            return next_name;
        }
    }
}

//===----------------------------------------------------------------------===//
//
// Code Generator
//
//===----------------------------------------------------------------------===//

CodeGen::CodeGen(ast::Context *context, catalog::CatalogAccessor *accessor)
    : context_(context)
    , position_{0, 0}
    , num_cached_scopes_(0)
    , scope_(nullptr)
    , accessor_(accessor)
    , pipeline_operating_units_(std::make_unique<selfdriving::PipelineOperatingUnits>()) {
    for (auto &scope : scope_cache_) {
        scope = std::make_unique<Scope>(nullptr);
    }
    num_cached_scopes_ = DEFAULT_SCOPE_CACHE_SIZE;
    EnterScope();
}

CodeGen::~CodeGen() {
    ExitScope();
}

auto CodeGen::ConstBool(bool val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewBoolLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return expr;
}

auto CodeGen::Const8(int8_t val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int8));
    return expr;
}

auto CodeGen::Const16(int16_t val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int16));
    return expr;
}

auto CodeGen::Const32(int32_t val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int32));
    return expr;
}

auto CodeGen::Const64(int64_t val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int64));
    return expr;
}

auto CodeGen::ConstU32(uint32_t val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint32));
    return expr;
}

auto CodeGen::ConstDouble(double val) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewFloatLiteral(position_, val);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float64));
    return expr;
}

auto CodeGen::ConstString(std::string_view str) const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewStringLiteral(position_, MakeIdentifier(str));
    expr->SetType(ast::StringType::Get(context_));
    return expr;
}

auto CodeGen::ConstNull(execution::sql::SqlTypeId type) const -> ast::Expr * {
    ast::Expr *dummy_expr;
    // initSqlNull(&expr) produces a NULL of expr's type.
    switch (type) {
    case execution::sql::SqlTypeId::Boolean:
        dummy_expr = BoolToSql(false);
        break;
    case execution::sql::SqlTypeId::TinyInt:  // fallthrough
    case execution::sql::SqlTypeId::SmallInt: // fallthrough
    case execution::sql::SqlTypeId::Integer:  // fallthrough
    case execution::sql::SqlTypeId::BigInt:
        dummy_expr = IntToSql(0);
        break;
    case execution::sql::SqlTypeId::Date:
        dummy_expr = DateToSql(0, 0, 0);
        break;
    case execution::sql::SqlTypeId::Timestamp:
        dummy_expr = TimestampToSql(0);
        break;
    case execution::sql::SqlTypeId::Varchar:
        dummy_expr = StringToSql("");
        break;
    case execution::sql::SqlTypeId::Double:
        dummy_expr = FloatToSql(0.0);
        break;
    case execution::sql::SqlTypeId::Varbinary:
    default:
        UNREACHABLE("Unsupported NULL type!");
    }
    return CallBuiltin(ast::Builtin::InitSqlNull, {PointerType(dummy_expr)});
}

auto CodeGen::DeclareVar(ast::Identifier name, ast::Expr *type_repr, ast::Expr *init) -> ast::VariableDecl * {
    // Create a unique name for the variable
    ast::IdentifierExpr *var_name = MakeExpr(name);
    // Build and append the declaration
    return context_->GetNodeFactory()->NewVariableDecl(position_, var_name->Name(), type_repr, init);
}

auto CodeGen::DeclareVarNoInit(ast::Identifier name, ast::Expr *type_repr) -> ast::VariableDecl * {
    return DeclareVar(name, type_repr, nullptr);
}

auto CodeGen::DeclareVarNoInit(ast::Identifier name, ast::BuiltinType::Kind kind) -> ast::VariableDecl * {
    return DeclareVarNoInit(name, BuiltinType(kind));
}

auto CodeGen::DeclareVarWithInit(ast::Identifier name, ast::Expr *init) -> ast::VariableDecl * {
    return DeclareVar(name, nullptr, init);
}

auto CodeGen::DeclareStruct(ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&fields) const
    -> ast::StructDecl * {
    auto type_repr = context_->GetNodeFactory()->NewStructType(position_, std::move(fields));
    return context_->GetNodeFactory()->NewStructDecl(position_, name, type_repr);
}

auto CodeGen::Assign(ast::Expr *dest, ast::Expr *value) -> ast::Stmt * {
    // TODO(pmenon): Check types?
    // Set the type of the destination
    dest->SetType(value->GetType());
    // Done.
    return context_->GetNodeFactory()->NewAssignmentStmt(position_, dest, value);
}

auto CodeGen::BuiltinType(ast::BuiltinType::Kind builtin_kind) const -> ast::Expr * {
    // Lookup the builtin type. We'll use it to construct an identifier.
    ast::BuiltinType *type = ast::BuiltinType::Get(context_, builtin_kind);
    // Build an identifier expression using the builtin's name
    ast::Expr *expr = MakeExpr(context_->GetIdentifier(type->GetTplName()));
    // Set the type to avoid double-checking the type
    expr->SetType(type);
    // Done
    return expr;
}

auto CodeGen::BoolType() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Bool);
}

auto CodeGen::Int8Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Int8);
}

auto CodeGen::Int16Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Int16);
}

auto CodeGen::Int32Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Int32);
}

auto CodeGen::Int64Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Int64);
}

auto CodeGen::Uint32Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Uint32);
}

auto CodeGen::Float32Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Float32);
}

auto CodeGen::Float64Type() const -> ast::Expr * {
    return BuiltinType(ast::BuiltinType::Float64);
}

auto CodeGen::PointerType(ast::Expr *base_type_repr) const -> ast::Expr * {
    // Create the type representation
    auto *type_repr = context_->GetNodeFactory()->NewPointerType(position_, base_type_repr);
    // Set the actual TPL type
    if (base_type_repr->GetType() != nullptr) {
        type_repr->SetType(ast::PointerType::Get(base_type_repr->GetType()));
    }
    // Done
    return type_repr;
}

auto CodeGen::PointerType(ast::Identifier type_name) const -> ast::Expr * {
    return PointerType(MakeExpr(type_name));
}

auto CodeGen::PointerType(ast::BuiltinType::Kind builtin) const -> ast::Expr * {
    return PointerType(BuiltinType(builtin));
}

auto CodeGen::ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind) -> ast::Expr * {
    return GetFactory()->NewArrayType(position_, Const64(num_elems), BuiltinType(kind));
}

auto CodeGen::ArrayAccess(ast::Identifier arr, uint64_t idx) -> ast::Expr * {
    return GetFactory()->NewIndexExpr(position_, MakeExpr(arr), Const64(idx));
}

auto CodeGen::TplType(sql::TypeId type) -> ast::Expr * {
    switch (type) {
    case sql::TypeId::Boolean:
        return BuiltinType(ast::BuiltinType::Boolean);
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
        return BuiltinType(ast::BuiltinType::Integer);
    case sql::TypeId::Date:
        return BuiltinType(ast::BuiltinType::Date);
    case sql::TypeId::Timestamp:
        return BuiltinType(ast::BuiltinType::Timestamp);
    case sql::TypeId::Double:
    case sql::TypeId::Float:
        return BuiltinType(ast::BuiltinType::Real);
    case sql::TypeId::Varchar:
    case sql::TypeId::Varbinary:
        return BuiltinType(ast::BuiltinType::StringVal);
    default:
        UNREACHABLE("Cannot codegen unsupported type.");
    }
}

auto CodeGen::AggregateType(parser::ExpressionType agg_type, sql::TypeId ret_type, sql::TypeId child_type) const
    -> ast::Expr * {
    switch (agg_type) {
    case parser::ExpressionType::AGGREGATE_COUNT:
        return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case parser::ExpressionType::AGGREGATE_AVG:
        return BuiltinType(ast::BuiltinType::AvgAggregate);
    case parser::ExpressionType::AGGREGATE_MIN:
        if (IsTypeIntegral(ret_type)) {
            return BuiltinType(ast::BuiltinType::IntegerMinAggregate);
        } else if (IsTypeFloatingPoint(ret_type)) {
            return BuiltinType(ast::BuiltinType::RealMinAggregate);
        } else if (ret_type == sql::TypeId::Date) {
            return BuiltinType(ast::BuiltinType::DateMinAggregate);
        } else if (ret_type == sql::TypeId::Varchar) {
            return BuiltinType(ast::BuiltinType::StringMinAggregate);
        } else {
            throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("MIN() aggregates on type {}", TypeIdToString(ret_type)));
        }
    case parser::ExpressionType::AGGREGATE_MAX:
        if (IsTypeIntegral(ret_type)) {
            return BuiltinType(ast::BuiltinType::IntegerMaxAggregate);
        } else if (IsTypeFloatingPoint(ret_type)) {
            return BuiltinType(ast::BuiltinType::RealMaxAggregate);
        } else if (ret_type == sql::TypeId::Date) {
            return BuiltinType(ast::BuiltinType::DateMaxAggregate);
        } else if (ret_type == sql::TypeId::Varchar) {
            return BuiltinType(ast::BuiltinType::StringMaxAggregate);
        } else {
            throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("MAX() aggregates on type {}", TypeIdToString(ret_type)));
        }
    case parser::ExpressionType::AGGREGATE_SUM:
        NOISEPAGE_ASSERT(IsTypeNumeric(ret_type), "Only arithmetic types have sums.");
        if (IsTypeIntegral(ret_type)) {
            return BuiltinType(ast::BuiltinType::IntegerSumAggregate);
        }
        return BuiltinType(ast::BuiltinType::RealSumAggregate);
    case parser::ExpressionType::AGGREGATE_TOP_K:
        if (child_type == sql::TypeId::Boolean) {
            return BuiltinType(ast::BuiltinType::BooleanTopKAggregate);
        } else if (IsTypeIntegral(child_type)) {
            return BuiltinType(ast::BuiltinType::IntegerTopKAggregate);
        } else if (IsTypeFloatingPoint(child_type)) {
            return BuiltinType(ast::BuiltinType::RealTopKAggregate);
        } else if (child_type == sql::TypeId::Varchar || child_type == sql::TypeId::Varbinary) {
            return BuiltinType(ast::BuiltinType::StringTopKAggregate);
        } else if (child_type == sql::TypeId::Date) {
            return BuiltinType(ast::BuiltinType::DateTopKAggregate);
        } else if (child_type == sql::TypeId::Timestamp) {
            return BuiltinType(ast::BuiltinType::TimestampTopKAggregate);
        } else {
            throw NOT_IMPLEMENTED_EXCEPTION(
                fmt::format("TOP K aggregate does not support type {}", TypeIdToString(child_type)));
        }
    case parser::ExpressionType::AGGREGATE_HISTOGRAM:
        if (child_type == sql::TypeId::Boolean) {
            return BuiltinType(ast::BuiltinType::BooleanHistogramAggregate);
        } else if (IsTypeIntegral(child_type)) {
            return BuiltinType(ast::BuiltinType::IntegerHistogramAggregate);
        } else if (IsTypeFloatingPoint(child_type)) {
            return BuiltinType(ast::BuiltinType::RealHistogramAggregate);
        } else if (child_type == sql::TypeId::Varchar || child_type == sql::TypeId::Varbinary) {
            return BuiltinType(ast::BuiltinType::StringHistogramAggregate);
        } else if (child_type == sql::TypeId::Date) {
            return BuiltinType(ast::BuiltinType::DateHistogramAggregate);
        } else if (child_type == sql::TypeId::Timestamp) {
            return BuiltinType(ast::BuiltinType::TimestampHistogramAggregate);
        } else {
            throw NOT_IMPLEMENTED_EXCEPTION(
                fmt::format("HISTOGRAM aggregate does not support type {}", TypeIdToString(child_type)));
        }
    default: {
        UNREACHABLE("AggregateType() should only be called with aggregates.");
    }
    }
}

auto CodeGen::Nil() const -> ast::Expr * {
    ast::Expr *expr = context_->GetNodeFactory()->NewNilLiteral(position_);
    expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return expr;
}

auto CodeGen::AddressOf(ast::Expr *obj) const -> ast::Expr * {
    return UnaryOp(parsing::Token::Type::AMPERSAND, obj);
}

auto CodeGen::AddressOf(ast::Identifier obj_name) const -> ast::Expr * {
    return UnaryOp(parsing::Token::Type::AMPERSAND, MakeExpr(obj_name));
}

auto CodeGen::SizeOf(ast::Identifier type_name) const -> ast::Expr * {
    return CallBuiltin(ast::Builtin::SizeOf, {MakeExpr(type_name)});
}

auto CodeGen::OffsetOf(ast::Identifier obj, ast::Identifier member) const -> ast::Expr * {
    return CallBuiltin(ast::Builtin::OffsetOf, {MakeExpr(obj), MakeExpr(member)});
}

auto CodeGen::PtrCast(ast::Expr *base, ast::Expr *arg) const -> ast::Expr * {
    ast::Expr *ptr = context_->GetNodeFactory()->NewUnaryOpExpr(position_, parsing::Token::Type::STAR, base);
    return CallBuiltin(ast::Builtin::PtrCast, {ptr, arg});
}

auto CodeGen::PtrCast(ast::Identifier base_name, ast::Expr *arg) const -> ast::Expr * {
    return PtrCast(MakeExpr(base_name), arg);
}

auto CodeGen::BinaryOp(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const -> ast::Expr * {
    NOISEPAGE_ASSERT(parsing::Token::IsBinaryOp(op), "Provided operation isn't binary");
    return context_->GetNodeFactory()->NewBinaryOpExpr(position_, op, left, right);
}

auto CodeGen::Compare(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const -> ast::Expr * {
    return context_->GetNodeFactory()->NewComparisonOpExpr(position_, op, left, right);
}

auto CodeGen::IsNilPointer(ast::Expr *obj) const -> ast::Expr * {
    return Compare(parsing::Token::Type::EQUAL_EQUAL, obj, Nil());
}

auto CodeGen::UnaryOp(parsing::Token::Type op, ast::Expr *input) const -> ast::Expr * {
    return context_->GetNodeFactory()->NewUnaryOpExpr(position_, op, input);
}

auto CodeGen::AccessStructMember(ast::Expr *object, ast::Identifier member) -> ast::Expr * {
    return context_->GetNodeFactory()->NewMemberExpr(position_, object, MakeExpr(member));
}

auto CodeGen::Return() -> ast::Stmt * {
    return Return(nullptr);
}

auto CodeGen::Return(ast::Expr *ret) -> ast::Stmt * {
    ast::Stmt *stmt = context_->GetNodeFactory()->NewReturnStmt(position_, ret);
    NewLine();
    return stmt;
}

auto CodeGen::Call(ast::Identifier func_name, std::initializer_list<ast::Expr *> args) const -> ast::Expr * {
    util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
    return context_->GetNodeFactory()->NewCallExpr(MakeExpr(func_name), std::move(call_args));
}

auto CodeGen::Call(ast::Identifier func_name, const std::vector<ast::Expr *> &args) const -> ast::Expr * {
    util::RegionVector<ast::Expr *> call_args(args.begin(), args.end(), context_->GetRegion());
    return context_->GetNodeFactory()->NewCallExpr(MakeExpr(func_name), std::move(call_args));
}

auto CodeGen::CallBuiltin(ast::Builtin builtin, std::initializer_list<ast::Expr *> args) const -> ast::Expr * {
    util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
    ast::Expr                      *func = MakeExpr(context_->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
    ast::Expr                      *call = context_->GetNodeFactory()->NewBuiltinCallExpr(func, std::move(call_args));
    return call;
}

// This is copied from the overloaded function. But, we use initializer so often we keep it around.
auto CodeGen::CallBuiltin(ast::Builtin builtin, const std::vector<ast::Expr *> &args) const -> ast::Expr * {
    util::RegionVector<ast::Expr *> call_args(args.begin(), args.end(), context_->GetRegion());
    ast::Expr                      *func = MakeExpr(context_->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
    ast::Expr                      *call = context_->GetNodeFactory()->NewBuiltinCallExpr(func, std::move(call_args));
    return call;
}

auto CodeGen::BoolToSql(bool b) const -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::BoolToSql, {ConstBool(b)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Boolean));
    return call;
}

auto CodeGen::IntToSql(int64_t num) const -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::IntToSql, {Const64(num)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Integer));
    return call;
}

auto CodeGen::FloatToSql(double num) const -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::FloatToSql, {ConstDouble(num)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Real));
    return call;
}

auto CodeGen::DateToSql(sql::Date date) const -> ast::Expr * {
    int32_t year, month, day;
    date.ExtractComponents(&year, &month, &day);
    return DateToSql(year, month, day);
}

auto CodeGen::DateToSql(int32_t year, int32_t month, int32_t day) const -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::DateToSql, {Const32(year), Const32(month), Const32(day)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Date));
    return call;
}

auto CodeGen::TimestampToSql(sql::Timestamp timestamp) const -> ast::Expr * {
    int32_t year, month, day, hour, min, sec, millisec, microsec;
    timestamp.ExtractComponents(&year, &month, &day, &hour, &min, &sec, &millisec, &microsec);
    ast::Expr *call = CallBuiltin(ast::Builtin::TimestampToSqlYMDHMSMU,
                                  {Const32(year),
                                   Const32(month),
                                   Const32(day),
                                   Const32(hour),
                                   Const32(min),
                                   Const32(sec),
                                   Const32(millisec),
                                   Const32(microsec)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Timestamp));
    return call;
}

auto CodeGen::TimestampToSql(uint64_t julian_usec) const -> ast::Expr * {
    ast::Expr *usec_expr = Const64(julian_usec);
    ast::Expr *call = CallBuiltin(ast::Builtin::TimestampToSql, {usec_expr});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Timestamp));
    return call;
}

auto CodeGen::StringToSql(std::string_view str) const -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::StringToSql, {ConstString(str)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::StringVal));
    return call;
}

auto CodeGen::IndexIteratorInit(ast::Identifier iter,
                                ast::Expr      *exec_ctx_var,
                                uint32_t        num_attrs,
                                uint32_t        table_oid,
                                uint32_t        index_oid,
                                ast::Identifier col_oids) -> ast::Expr * {
    // @indexIteratorInit(&iter, table_oid, index_oid, execCtx)
    return IndexIteratorInit(AddressOf(iter), exec_ctx_var, num_attrs, table_oid, index_oid, col_oids);
}

auto CodeGen::IndexIteratorInit(ast::Expr      *iter_ptr,
                                ast::Expr      *exec_ctx_var,
                                uint32_t        num_attrs,
                                uint32_t        table_oid,
                                uint32_t        index_oid,
                                ast::Identifier col_oids) -> ast::Expr * {
    // @indexIteratorInit(iter_ptr, table_oid, index_oid, execCtx)
    ast::Expr               *num_attrs_expr = Const32(static_cast<int32_t>(num_attrs));
    ast::Expr               *table_oid_expr = Const32(static_cast<int32_t>(table_oid));
    ast::Expr               *index_oid_expr = Const32(static_cast<int32_t>(index_oid));
    ast::Expr               *col_oids_expr = MakeExpr(col_oids);
    std::vector<ast::Expr *> args{iter_ptr,
                                  exec_ctx_var,
                                  num_attrs_expr,
                                  table_oid_expr,
                                  index_oid_expr,
                                  col_oids_expr};
    return CallBuiltin(ast::Builtin::IndexIteratorInit, args);
}

auto CodeGen::IndexIteratorScan(ast::Identifier iter, planner::IndexScanType scan_type, uint32_t limit) -> ast::Expr * {
    return IndexIteratorScan(AddressOf(iter), scan_type, limit);
}

auto CodeGen::IndexIteratorScan(ast::Expr *iter_ptr, planner::IndexScanType scan_type, uint32_t limit) -> ast::Expr * {
    // @indexIteratorScanKey(iter_ptr)
    ast::Builtin             builtin;
    bool                     asc_scan = false;
    bool                     use_limit = false;
    storage::index::ScanType asc_type;
    switch (scan_type) {
    case planner::IndexScanType::Exact:
        builtin = ast::Builtin::IndexIteratorScanKey;
        break;
    case planner::IndexScanType::AscendingClosed:
    case planner::IndexScanType::AscendingOpenHigh:
    case planner::IndexScanType::AscendingOpenLow:
    case planner::IndexScanType::AscendingOpenBoth:
        asc_scan = true;
        use_limit = true;
        builtin = ast::Builtin::IndexIteratorScanAscending;
        if (scan_type == planner::IndexScanType::AscendingClosed) {
            asc_type = storage::index::ScanType::Closed;
        } else if (scan_type == planner::IndexScanType::AscendingOpenHigh) {
            asc_type = storage::index::ScanType::OpenHigh;
        } else if (scan_type == planner::IndexScanType::AscendingOpenLow) {
            asc_type = storage::index::ScanType::OpenLow;
        } else if (scan_type == planner::IndexScanType::AscendingOpenBoth) {
            asc_type = storage::index::ScanType::OpenBoth;
        }
        break;
    case planner::IndexScanType::Descending:
        builtin = ast::Builtin::IndexIteratorScanDescending;
        break;
    case planner::IndexScanType::DescendingLimit:
        use_limit = true;
        builtin = ast::Builtin::IndexIteratorScanLimitDescending;
        break;
    default:
        UNREACHABLE("Unknown scan type");
    }

    if (!use_limit && !asc_scan) {
        return CallBuiltin(builtin, {iter_ptr});
    }

    std::vector<ast::Expr *> args{iter_ptr};

    if (asc_scan) {
        args.push_back(Const64(static_cast<int64_t>(asc_type)));
    }
    if (use_limit) {
        args.push_back(Const32(limit));
    }

    return CallBuiltin(builtin, args);
}

auto CodeGen::PRGet(ast::Expr *pr, execution::sql::SqlTypeId type, bool nullable, uint32_t attr_idx) -> ast::Expr * {
    // @indexIteratorGetTypeNull(&iter, attr_idx)
    ast::Builtin builtin;
    switch (type) {
    case execution::sql::SqlTypeId::Boolean:
        builtin = nullable ? ast::Builtin::PRGetBoolNull : ast::Builtin::PRGetBool;
        break;
    case execution::sql::SqlTypeId::TinyInt:
        builtin = nullable ? ast::Builtin::PRGetTinyIntNull : ast::Builtin::PRGetTinyInt;
        break;
    case execution::sql::SqlTypeId::SmallInt:
        builtin = nullable ? ast::Builtin::PRGetSmallIntNull : ast::Builtin::PRGetSmallInt;
        break;
    case execution::sql::SqlTypeId::Integer:
        builtin = nullable ? ast::Builtin::PRGetIntNull : ast::Builtin::PRGetInt;
        break;
    case execution::sql::SqlTypeId::BigInt:
        builtin = nullable ? ast::Builtin::PRGetBigIntNull : ast::Builtin::PRGetBigInt;
        break;
    case execution::sql::SqlTypeId::Double:
        builtin = nullable ? ast::Builtin::PRGetDoubleNull : ast::Builtin::PRGetDouble;
        break;
    case execution::sql::SqlTypeId::Date:
        builtin = nullable ? ast::Builtin::PRGetDateNull : ast::Builtin::PRGetDate;
        break;
    case execution::sql::SqlTypeId::Timestamp:
        builtin = nullable ? ast::Builtin::PRGetTimestampNull : ast::Builtin::PRGetTimestamp;
        break;
    case execution::sql::SqlTypeId::Varchar:
    case execution::sql::SqlTypeId::Varbinary:
        builtin = nullable ? ast::Builtin::PRGetVarlenNull : ast::Builtin::PRGetVarlen;
        break;
    default:
        // TODO(amlatyr): Support other types.
        UNREACHABLE("Unsupported index get type!");
    }
    ast::Expr *idx_expr = GetFactory()->NewIntLiteral(position_, attr_idx);
    return CallBuiltin(builtin, {pr, idx_expr});
}

auto CodeGen::PRSet(ast::Expr                *pr,
                    execution::sql::SqlTypeId type,
                    bool                      nullable,
                    uint32_t                  attr_idx,
                    ast::Expr                *val,
                    bool                      own) -> ast::Expr * {
    ast::Builtin builtin;
    switch (type) {
    case execution::sql::SqlTypeId::Boolean:
        builtin = nullable ? ast::Builtin::PRSetBoolNull : ast::Builtin::PRSetBool;
        break;
    case execution::sql::SqlTypeId::TinyInt:
        builtin = nullable ? ast::Builtin::PRSetTinyIntNull : ast::Builtin::PRSetTinyInt;
        break;
    case execution::sql::SqlTypeId::SmallInt:
        builtin = nullable ? ast::Builtin::PRSetSmallIntNull : ast::Builtin::PRSetSmallInt;
        break;
    case execution::sql::SqlTypeId::Integer:
        builtin = nullable ? ast::Builtin::PRSetIntNull : ast::Builtin::PRSetInt;
        break;
    case execution::sql::SqlTypeId::BigInt:
        builtin = nullable ? ast::Builtin::PRSetBigIntNull : ast::Builtin::PRSetBigInt;
        break;
    case execution::sql::SqlTypeId::Double:
        builtin = nullable ? ast::Builtin::PRSetDoubleNull : ast::Builtin::PRSetDouble;
        break;
    case execution::sql::SqlTypeId::Date:
        builtin = nullable ? ast::Builtin::PRSetDateNull : ast::Builtin::PRSetDate;
        break;
    case execution::sql::SqlTypeId::Timestamp:
        builtin = nullable ? ast::Builtin::PRSetTimestampNull : ast::Builtin::PRSetTimestamp;
        break;
    case execution::sql::SqlTypeId::Varchar:
    case execution::sql::SqlTypeId::Varbinary:
        builtin = nullable ? ast::Builtin::PRSetVarlenNull : ast::Builtin::PRSetVarlen;
        break;
    default:
        UNREACHABLE("Unsupported index set type!");
    }
    ast::Expr *idx_expr = GetFactory()->NewIntLiteral(position_, attr_idx);
    if (builtin == ast::Builtin::PRSetVarlenNull || builtin == ast::Builtin::PRSetVarlen) {
        return CallBuiltin(builtin, {pr, idx_expr, val, ConstBool(own)});
    }
    return CallBuiltin(builtin, {pr, idx_expr, val});
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

auto CodeGen::GetCatalogAccessor() const -> catalog::CatalogAccessor * {
    return accessor_;
}

auto CodeGen::TableIterInit(ast::Expr           *table_iter,
                            ast::Expr           *exec_ctx,
                            catalog::table_oid_t table_oid,
                            ast::Identifier      col_oids) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::TableIterInit,
                                  {table_iter, exec_ctx, Const32(table_oid.UnderlyingValue()), MakeExpr(col_oids)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::TempTableIterInit(ast::Identifier tvi,
                                ast::Expr      *cte_scan_iterator_ptr,
                                ast::Identifier col_oids,
                                ast::Expr      *exec_ctx_expr) -> ast::Expr * {
    ast::Expr *tvi_ptr = MakeExpr(tvi);
    ast::Expr *col_oids_expr = MakeExpr(col_oids);

    std::vector<ast::Expr *> args{tvi_ptr, exec_ctx_expr, col_oids_expr, cte_scan_iterator_ptr};
    return CallBuiltin(ast::Builtin::TempTableIterInitBind, args);
}

auto CodeGen::TableIterAdvance(ast::Expr *table_iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::TableIterAdvance, {table_iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::TableIterGetVPI(ast::Expr *table_iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::TableIterGetVPI, {table_iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::VectorProjectionIterator)->PointerTo());
    return call;
}

auto CodeGen::TableIterClose(ast::Expr *table_iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::TableIterClose, {table_iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::IterateTableParallel(catalog::table_oid_t table_oid,
                                   ast::Identifier      col_oids,
                                   ast::Expr           *query_state,
                                   ast::Expr           *exec_ctx,
                                   ast::Identifier      worker_name) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::TableIterParallel,
                                  {Const32(table_oid.UnderlyingValue()),
                                   MakeExpr(col_oids),
                                   query_state,
                                   exec_ctx,
                                   Const32(0),
                                   MakeExpr(worker_name)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AbortTxn(ast::Expr *exec_ctx) -> ast::Expr * {
    return CallBuiltin(ast::Builtin::AbortTxn, {exec_ctx});
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

auto CodeGen::VPIIsFiltered(ast::Expr *vpi) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::VPIIsFiltered, {vpi});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::VPIHasNext(ast::Expr *vpi, bool filtered) -> ast::Expr * {
    ast::Builtin builtin = filtered ? ast::Builtin::VPIHasNextFiltered : ast::Builtin::VPIHasNext;
    ast::Expr   *call = CallBuiltin(builtin, {vpi});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::VPIAdvance(ast::Expr *vpi, bool filtered) -> ast::Expr * {
    ast::Builtin builtin = filtered ? ast::Builtin ::VPIAdvanceFiltered : ast::Builtin ::VPIAdvance;
    ast::Expr   *call = CallBuiltin(builtin, {vpi});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::VPIMatch(ast::Expr *vpi, ast::Expr *cond) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::VPIMatch, {vpi, cond});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::VPIInit(ast::Expr *vpi, ast::Expr *vp, ast::Expr *tids) -> ast::Expr * {
    ast::Expr *call = nullptr;
    if (tids != nullptr) {
        call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp, tids});
    } else {
        call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp});
    }
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::VPIGet(ast::Expr *vpi, sql::TypeId type_id, bool nullable, uint32_t idx) -> ast::Expr * {
    ast::Builtin           builtin;
    ast::BuiltinType::Kind ret_kind;
    switch (type_id) {
    case sql::TypeId::Boolean:
        builtin = nullable ? ast::Builtin::VPIGetBoolNull : ast::Builtin::VPIGetBool;
        ret_kind = ast::BuiltinType::Boolean;
        break;
    case sql::TypeId::TinyInt:
        builtin = nullable ? ast::Builtin::VPIGetTinyIntNull : ast::Builtin::VPIGetTinyInt;
        ret_kind = ast::BuiltinType::Integer;
        break;
    case sql::TypeId::SmallInt:
        builtin = nullable ? ast::Builtin::VPIGetSmallIntNull : ast::Builtin::VPIGetSmallInt;
        ret_kind = ast::BuiltinType::Integer;
        break;
    case sql::TypeId::Integer:
        builtin = nullable ? ast::Builtin::VPIGetIntNull : ast::Builtin::VPIGetInt;
        ret_kind = ast::BuiltinType::Integer;
        break;
    case sql::TypeId::BigInt:
        builtin = nullable ? ast::Builtin::VPIGetBigIntNull : ast::Builtin::VPIGetBigInt;
        ret_kind = ast::BuiltinType::Integer;
        break;
    case sql::TypeId::Float:
        builtin = nullable ? ast::Builtin::VPIGetRealNull : ast::Builtin::VPIGetReal;
        ret_kind = ast::BuiltinType::Real;
        break;
    case sql::TypeId::Double:
        builtin = nullable ? ast::Builtin::VPIGetDoubleNull : ast::Builtin::VPIGetDouble;
        ret_kind = ast::BuiltinType::Real;
        break;
    case sql::TypeId::Date:
        builtin = nullable ? ast::Builtin::VPIGetDateNull : ast::Builtin::VPIGetDate;
        ret_kind = ast::BuiltinType::Date;
        break;
    case sql::TypeId::Timestamp:
        builtin = nullable ? ast::Builtin::VPIGetTimestampNull : ast::Builtin::VPIGetTimestamp;
        ret_kind = ast::BuiltinType::Timestamp;
        break;
    case sql::TypeId::Varchar:
    case sql::TypeId::Varbinary:
        builtin = nullable ? ast::Builtin::VPIGetStringNull : ast::Builtin::VPIGetString;
        ret_kind = ast::BuiltinType::StringVal;
        break;
    default:
        throw NOT_IMPLEMENTED_EXCEPTION(
            fmt::format("CodeGen: Reading type {} from VPI not supported.", TypeIdToString(type_id)));
    }
    ast::Expr *call = CallBuiltin(builtin, {vpi, Const32(idx)});
    call->SetType(ast::BuiltinType::Get(context_, ret_kind));
    return call;
}

auto CodeGen::VPIFilter(ast::Expr             *exec_ctx,
                        ast::Expr             *vp,
                        parser::ExpressionType comp_type,
                        uint32_t               col_idx,
                        ast::Expr             *filter_val,
                        ast::Expr             *tids) -> ast::Expr * {
    // Call @FilterComp(execCtx, vpi, col_idx, col_type, filter_val)
    ast::Builtin builtin;
    switch (comp_type) {
    case parser::ExpressionType::COMPARE_EQUAL:
        builtin = ast::Builtin::VectorFilterEqual;
        break;
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
        builtin = ast::Builtin::VectorFilterNotEqual;
        break;
    case parser::ExpressionType::COMPARE_LESS_THAN:
        builtin = ast::Builtin::VectorFilterLessThan;
        break;
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
        builtin = ast::Builtin::VectorFilterLessThanEqual;
        break;
    case parser::ExpressionType::COMPARE_GREATER_THAN:
        builtin = ast::Builtin::VectorFilterGreaterThan;
        break;
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
        builtin = ast::Builtin::VectorFilterGreaterThanEqual;
        break;
    case parser::ExpressionType::COMPARE_LIKE:
        builtin = ast::Builtin::VectorFilterLike;
        break;
    case parser::ExpressionType::COMPARE_NOT_LIKE:
        builtin = ast::Builtin::VectorFilterNotLike;
        break;
    default:
        throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("CodeGen: Vector filter type {} from VPI not supported.",
                                                    parser::ExpressionTypeToString(comp_type)));
    }
    ast::Expr *call = CallBuiltin(builtin, {exec_ctx, vp, Const32(col_idx), filter_val, tids});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

auto CodeGen::FilterManagerInit(ast::Expr *filter_manager, ast::Expr *exec_ctx) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerInit, {filter_manager, exec_ctx});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::FilterManagerFree(ast::Expr *filter_manager) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerFree, {filter_manager});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::FilterManagerInsert(ast::Expr *filter_manager, const std::vector<ast::Identifier> &clause_fn_names)
    -> ast::Expr * {
    std::vector<ast::Expr *> params(1 + clause_fn_names.size());
    params[0] = filter_manager;
    for (uint32_t i = 0; i < clause_fn_names.size(); i++) {
        params[i + 1] = MakeExpr(clause_fn_names[i]);
    }
    ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerInsertFilter, params);
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::FilterManagerRunFilters(ast::Expr *filter_manager, ast::Expr *vpi, ast::Expr *exec_ctx) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerRunFilters, {filter_manager, vpi, exec_ctx});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecCtxRegisterHook(ast::Expr *exec_ctx, uint32_t hook_idx, ast::Identifier hook) -> ast::Expr * {
    ast::Expr *call
        = CallBuiltin(ast::Builtin::ExecutionContextRegisterHook, {exec_ctx, Const32(hook_idx), MakeExpr(hook)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecCtxClearHooks(ast::Expr *exec_ctx) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextClearHooks, {exec_ctx});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecCtxInitHooks(ast::Expr *exec_ctx, uint32_t num_hooks) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextInitHooks, {exec_ctx, Const32(num_hooks)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecCtxAddRowsAffected(ast::Expr *exec_ctx, int64_t num_rows_affected) -> ast::Expr * {
    ast::Expr *call
        = CallBuiltin(ast::Builtin::ExecutionContextAddRowsAffected, {exec_ctx, Const64(num_rows_affected)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecOUFeatureVectorRecordFeature(ast::Expr                                           *ouvec,
                                               pipeline_id_t                                        pipeline_id,
                                               feature_id_t                                         feature_id,
                                               selfdriving::ExecutionOperatingUnitFeatureAttribute  feature_attribute,
                                               selfdriving::ExecutionOperatingUnitFeatureUpdateMode mode,
                                               ast::Expr *value) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ExecOUFeatureVectorRecordFeature,
                                  {ouvec,
                                   Const32(pipeline_id.UnderlyingValue()),
                                   Const32(feature_id.UnderlyingValue()),
                                   Const32(static_cast<int32_t>(feature_attribute)),
                                   Const32(static_cast<int32_t>(mode)),
                                   value});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::ExecCtxGetMemoryPool(ast::Expr *exec_ctx) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetMemoryPool, {exec_ctx});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::MemoryPool)->PointerTo());
    return call;
}

auto CodeGen::ExecCtxGetTLS(ast::Expr *exec_ctx) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetTLS, {exec_ctx});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::ThreadStateContainer)->PointerTo());
    return call;
}

auto CodeGen::TLSAccessCurrentThreadState(ast::Expr *tls, ast::Identifier state_type_name) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerGetState, {tls});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(state_type_name, call);
}

auto CodeGen::TLSIterate(ast::Expr *tls, ast::Expr *context, ast::Identifier func) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerIterate, {tls, context, MakeExpr(func)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::TLSReset(ast::Expr      *tls,
                       ast::Identifier tls_state_name,
                       ast::Identifier init_fn,
                       ast::Identifier tear_down_fn,
                       ast::Expr      *context) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerReset,
                                  {tls, SizeOf(tls_state_name), MakeExpr(init_fn), MakeExpr(tear_down_fn), context});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::TLSClear(ast::Expr *tls) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerClear, {tls});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Hash
// ---------------------------------------------------------

auto CodeGen::Hash(const std::vector<ast::Expr *> &values) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::Hash, values);
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint64));
    return call;
}

// ---------------------------------------------------------
// Joins
// ---------------------------------------------------------

auto CodeGen::JoinHashTableInit(ast::Expr *join_hash_table, ast::Expr *exec_ctx, ast::Identifier build_row_type_name)
    -> ast::Expr * {
    ast::Expr *call
        = CallBuiltin(ast::Builtin::JoinHashTableInit, {join_hash_table, exec_ctx, SizeOf(build_row_type_name)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHashTableInsert(ast::Expr *join_hash_table, ast::Expr *hash_val, ast::Identifier tuple_type_name)
    -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableInsert, {join_hash_table, hash_val});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return PtrCast(tuple_type_name, call);
}

auto CodeGen::JoinHashTableBuild(ast::Expr *join_hash_table) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableBuild, {join_hash_table});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHashTableBuildParallel(ast::Expr *join_hash_table,
                                         ast::Expr *thread_state_container,
                                         ast::Expr *offset) -> ast::Expr * {
    ast::Expr *call
        = CallBuiltin(ast::Builtin::JoinHashTableBuildParallel, {join_hash_table, thread_state_container, offset});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHashTableLookup(ast::Expr *join_hash_table, ast::Expr *entry_iter, ast::Expr *hash_val)
    -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableLookup, {join_hash_table, entry_iter, hash_val});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHashTableFree(ast::Expr *join_hash_table) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableFree, {join_hash_table});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::HTEntryIterHasNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::HashTableEntryIterHasNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::HTEntryIterGetRow(ast::Expr *iter, ast::Identifier row_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::HashTableEntryIterGetRow, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(row_type, call);
}

auto CodeGen::JoinHTIteratorInit(ast::Expr *iter, ast::Expr *ht) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterInit, {iter, ht});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHTIteratorHasNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterHasNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::JoinHTIteratorNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::JoinHTIteratorGetRow(ast::Expr *iter, ast::Identifier payload_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterGetRow, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(payload_type, call);
}

auto CodeGen::JoinHTIteratorFree(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterFree, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Hash aggregations
// ---------------------------------------------------------

auto CodeGen::AggHashTableInit(ast::Expr *agg_ht, ast::Expr *exec_ctx, ast::Identifier agg_payload_type)
    -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableInit, {agg_ht, exec_ctx, SizeOf(agg_payload_type)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableLookup(ast::Expr      *agg_ht,
                                 ast::Expr      *hash_val,
                                 ast::Identifier key_check,
                                 ast::Expr      *input,
                                 ast::Identifier agg_payload_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableLookup, {agg_ht, hash_val, MakeExpr(key_check), input});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(agg_payload_type, call);
}

auto CodeGen::AggHashTableInsert(ast::Expr      *agg_ht,
                                 ast::Expr      *hash_val,
                                 bool            partitioned,
                                 ast::Identifier agg_payload_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableInsert, {agg_ht, hash_val, ConstBool(partitioned)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(agg_payload_type, call);
}

auto CodeGen::AggHashTableLinkEntry(ast::Expr *agg_ht, ast::Expr *entry) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableLinkEntry, {agg_ht, entry});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableMovePartitions(ast::Expr      *agg_ht,
                                         ast::Expr      *tls,
                                         ast::Expr      *tl_agg_ht_offset,
                                         ast::Identifier merge_partitions_fn_name) -> ast::Expr * {
    std::initializer_list<ast::Expr *> args = {agg_ht, tls, tl_agg_ht_offset, MakeExpr(merge_partitions_fn_name)};
    ast::Expr                         *call = CallBuiltin(ast::Builtin::AggHashTableMovePartitions, args);
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableParallelScan(ast::Expr      *agg_ht,
                                       ast::Expr      *query_state,
                                       ast::Expr      *thread_state_container,
                                       ast::Identifier worker_fn) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableParallelPartitionedScan,
                                  {agg_ht, query_state, thread_state_container, MakeExpr(worker_fn)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableFree(ast::Expr *agg_ht) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableFree, {agg_ht});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Aggregation Hash Table Overflow Iterator
// ---------------------------------------------------------

auto CodeGen::AggPartitionIteratorHasNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterHasNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::AggPartitionIteratorNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggPartitionIteratorGetHash(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetHash, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint64));
    return call;
}

auto CodeGen::AggPartitionIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetRow, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(agg_payload_type, call);
}

auto CodeGen::AggPartitionIteratorGetRowEntry(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetRowEntry, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::HashTableEntry)->PointerTo());
    return call;
}

auto CodeGen::AggHashTableIteratorInit(ast::Expr *iter, ast::Expr *agg_ht) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterInit, {iter, agg_ht});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableIteratorHasNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterHasNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::AggHashTableIteratorNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggHashTableIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterGetRow, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(agg_payload_type, call);
}

auto CodeGen::AggHashTableIteratorClose(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterClose, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Aggregators
// ---------------------------------------------------------

auto CodeGen::AggregatorInit(ast::Expr *agg) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggInit, {agg});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggregatorAdvance(ast::Expr *agg, ast::Expr *val) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggAdvance, {agg, val});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggregatorMerge(ast::Expr *agg1, ast::Expr *agg2) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggMerge, {agg1, agg2});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::AggregatorResult(ast::Expr *exec_ctx, ast::Expr *agg, const parser::ExpressionType &expression_type)
    -> ast::Expr * {
    if (expression_type == parser::ExpressionType::AGGREGATE_TOP_K
        || expression_type == parser::ExpressionType::AGGREGATE_HISTOGRAM) {
        return CallBuiltin(ast::Builtin::AggResult, {exec_ctx, agg});
    }
    return CallBuiltin(ast::Builtin::AggResult, {agg});
}

auto CodeGen::AggregatorFree(ast::Expr *agg) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::AggFree, {agg});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

auto CodeGen::SorterInit(ast::Expr      *sorter,
                         ast::Expr      *exec_ctx,
                         ast::Identifier cmp_func_name,
                         ast::Identifier sort_row_type_name) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterInit,
                                  {sorter, exec_ctx, MakeExpr(cmp_func_name), SizeOf(sort_row_type_name)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterInsert(ast::Expr *sorter, ast::Identifier sort_row_type_name) -> ast::Expr * {
    // @sorterInsert(sorter)
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsert, {sorter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    // @ptrCast(sort_row_type, @sorterInsert())
    return PtrCast(sort_row_type_name, call);
}

auto CodeGen::SorterInsertTopK(ast::Expr *sorter, ast::Identifier sort_row_type_name, uint64_t top_k) -> ast::Expr * {
    // @sorterInsertTopK(sorter)
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsertTopK, {sorter, Const64(top_k)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    // @ptrCast(sort_row_type, @sorterInsertTopK())
    return PtrCast(sort_row_type_name, call);
}

auto CodeGen::SorterInsertTopKFinish(ast::Expr *sorter, uint64_t top_k) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsertTopKFinish, {sorter, Const64(top_k)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterSort(ast::Expr *sorter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterSort, {sorter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SortParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterSortParallel, {sorter, tls, offset});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SortTopKParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset, std::size_t top_k) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterSortTopKParallel, {sorter, tls, offset, Const64(top_k)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterFree(ast::Expr *sorter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterFree, {sorter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterIterInit(ast::Expr *iter, ast::Expr *sorter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterInit, {iter, sorter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterIterHasNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterHasNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::SorterIterNext(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterNext, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterIterSkipRows(ast::Expr *iter, uint32_t n) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterSkipRows, {iter, Const64(n)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::SorterIterGetRow(ast::Expr *iter, ast::Identifier row_type_name) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterGetRow, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
    return PtrCast(row_type_name, call);
}

auto CodeGen::SorterIterClose(ast::Expr *iter) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterClose, {iter});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

// ---------------------------------------------------------
// SQL functions
// ---------------------------------------------------------

auto CodeGen::Like(ast::Expr *str, ast::Expr *pattern) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::Like, {str, pattern});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::NotLike(ast::Expr *str, ast::Expr *pattern) -> ast::Expr * {
    return UnaryOp(parsing::Token::Type::BANG, Like(str, pattern));
}

// ---------------------------------------------------------
// CSV
// ---------------------------------------------------------

auto CodeGen::CSVReaderInit(ast::Expr *reader, std::string_view file_name) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderInit, {reader, ConstString(file_name)});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::CSVReaderAdvance(ast::Expr *reader) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderAdvance, {reader});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
    return call;
}

auto CodeGen::CSVReaderGetField(ast::Expr *reader, uint32_t field_index, ast::Expr *result) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderGetField, {reader, Const32(field_index), result});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::CSVReaderGetRecordNumber(ast::Expr *reader) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderGetRecordNumber, {reader});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint32));
    return call;
}

auto CodeGen::CSVReaderClose(ast::Expr *reader) -> ast::Expr * {
    ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderClose, {reader});
    call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
    return call;
}

auto CodeGen::StorageInterfaceInit(ast::Expr      *storage_interface_ptr,
                                   ast::Expr      *exec_ctx,
                                   uint32_t        table_oid,
                                   ast::Identifier col_oids,
                                   bool            need_indexes) -> ast::Expr * {
    ast::Expr *table_oid_expr = Const64(static_cast<int64_t>(table_oid));
    ast::Expr *col_oids_expr = MakeExpr(col_oids);
    ast::Expr *need_indexes_expr = ConstBool(need_indexes);

    std::vector<ast::Expr *> args{storage_interface_ptr, exec_ctx, table_oid_expr, col_oids_expr, need_indexes_expr};
    return CallBuiltin(ast::Builtin::StorageInterfaceInit, args);
}

// ---------------------------------------------------------
// Extras
// ---------------------------------------------------------

auto CodeGen::MakeFreshIdentifier(const std::string &str) -> ast::Identifier {
    return context_->GetIdentifier(scope_->GetFreshName(str));
}

auto CodeGen::MakeIdentifier(std::string_view str) const -> ast::Identifier {
    return context_->GetIdentifier({str.data(), str.length()});
}

auto CodeGen::MakeExpr(ast::Identifier ident) const -> ast::IdentifierExpr * {
    return context_->GetNodeFactory()->NewIdentifierExpr(position_, ident);
}

auto CodeGen::MakeStmt(ast::Expr *expr) const -> ast::Stmt * {
    return context_->GetNodeFactory()->NewExpressionStmt(expr);
}

auto CodeGen::MakeEmptyBlock() const -> ast::BlockStmt * {
    return context_->GetNodeFactory()->NewBlockStmt(position_, position_, {{}, context_->GetRegion()});
}

auto CodeGen::MakeEmptyFieldList() const -> util::RegionVector<ast::FieldDecl *> {
    return util::RegionVector<ast::FieldDecl *>(context_->GetRegion());
}

auto CodeGen::MakeFieldList(std::initializer_list<ast::FieldDecl *> fields) const
    -> util::RegionVector<ast::FieldDecl *> {
    return util::RegionVector<ast::FieldDecl *>(fields, context_->GetRegion());
}

auto CodeGen::MakeField(ast::Identifier name, ast::Expr *type) const -> ast::FieldDecl * {
    return context_->GetNodeFactory()->NewFieldDecl(position_, name, type);
}

auto CodeGen::CteScanIteratorInit(ast::Expr           *csi,
                                  catalog::table_oid_t table,
                                  ast::Identifier      col_ids,
                                  ast::Identifier      col_types,
                                  ast::Expr           *exec_ctx_var) -> ast::Expr * {
    ast::Expr *col_oids_expr = MakeExpr(col_ids);
    ast::Expr *col_types_expr = MakeExpr(col_types);

    std::vector<ast::Expr *> args{csi, exec_ctx_var, Const32(table.UnderlyingValue()), col_oids_expr, col_types_expr};
    return CallBuiltin(ast::Builtin::CteScanInit, args);
}

auto CodeGen::IndCteScanIteratorInit(ast::Expr           *csi,
                                     catalog::table_oid_t table_oid,
                                     ast::Identifier      col_ids,
                                     ast::Identifier      col_types,
                                     bool                 is_recursive,
                                     ast::Expr           *exec_ctx_var) -> ast::Expr * {
    ast::Expr *col_ids_expr = MakeExpr(col_ids);
    ast::Expr *col_types_expr = MakeExpr(col_types);

    std::vector<ast::Expr *> args{csi,
                                  exec_ctx_var,
                                  Const32(table_oid.UnderlyingValue()),
                                  col_ids_expr,
                                  col_types_expr,
                                  ConstBool(is_recursive)};
    return CallBuiltin(ast::Builtin::IndCteScanInit, args);
}

auto CodeGen::GetFactory() -> ast::AstNodeFactory * {
    return context_->GetNodeFactory();
}

void CodeGen::EnterScope() {
    if (num_cached_scopes_ == 0) {
        scope_ = new Scope(scope_);
    } else {
        auto scope = scope_cache_[--num_cached_scopes_].release();
        scope->Init(scope_);
        scope_ = scope;
    }
}

void CodeGen::ExitScope() {
    Scope *scope = scope_;
    scope_ = scope->Previous();

    if (num_cached_scopes_ < DEFAULT_SCOPE_CACHE_SIZE) {
        scope_cache_[num_cached_scopes_++].reset(scope);
    } else {
        delete scope;
    }
}

} // namespace noisepage::execution::compiler
