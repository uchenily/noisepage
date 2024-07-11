#include "optimizer/logical_operators.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage::optimizer {

auto LeafOperator::Copy() const -> BaseOperatorNodeContents * {
    return new LeafOperator(*this);
}

auto LeafOperator::Make(group_id_t group) -> Operator {
    auto *leaf = new LeafOperator();
    leaf->origin_group_ = group;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(leaf));
}

auto LeafOperator::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(origin_group_));
    return hash;
}

auto LeafOperator::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LEAF) {
        return false;
    }
    const LeafOperator &node = *dynamic_cast<const LeafOperator *>(&r);
    return origin_group_ == node.origin_group_;
}

//===--------------------------------------------------------------------===//
// Logical Get
//===--------------------------------------------------------------------===//
auto LogicalGet::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalGet(*this);
}

auto LogicalGet::Make(catalog::db_oid_t                database_oid,
                      catalog::table_oid_t             table_oid,
                      std::vector<AnnotatedExpression> predicates,
                      parser::AliasType                table_alias,
                      bool                             is_for_update) -> Operator {
    auto *get = new LogicalGet();
    get->database_oid_ = database_oid;
    get->table_oid_ = table_oid;
    get->predicates_ = std::move(predicates);
    get->table_alias_ = std::move(table_alias);
    get->is_for_update_ = is_for_update;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

auto LogicalGet::Make() -> Operator {
    auto get = new LogicalGet();
    get->database_oid_ = catalog::INVALID_DATABASE_OID;
    get->table_oid_ = catalog::INVALID_TABLE_OID;
    get->is_for_update_ = false;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

auto LogicalGet::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashInRange(hash, predicates_.begin(), predicates_.end());
    hash = common::HashUtil::CombineHashes(hash, std::hash<parser::AliasType>{}(table_alias_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
    return hash;
}

auto LogicalGet::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALGET) {
        return false;
    }
    const LogicalGet &node = *dynamic_cast<const LogicalGet *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (predicates_.size() != node.predicates_.size()) {
        return false;
    }
    for (size_t i = 0; i < predicates_.size(); i++) {
        if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) {
            return false;
        }
    }
    if (table_alias_ != node.table_alias_) {
        return false;
    }
    return is_for_update_ == node.is_for_update_;
}

//===--------------------------------------------------------------------===//
// External file get
//===--------------------------------------------------------------------===//
auto LogicalExternalFileGet::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalExternalFileGet(*this);
}

auto LogicalExternalFileGet::Make(parser::ExternalFileFormat format,
                                  std::string                file_name,
                                  char                       delimiter,
                                  char                       quote,
                                  char                       escape) -> Operator {
    auto get = new LogicalExternalFileGet();
    get->format_ = format;
    get->file_name_ = std::move(file_name);
    get->delimiter_ = delimiter;
    get->quote_ = quote;
    get->escape_ = escape;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

auto LogicalExternalFileGet::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALEXTERNALFILEGET) {
        return false;
    }
    const auto &get = *static_cast<const LogicalExternalFileGet *>(&r);
    return (format_ == get.format_ && file_name_ == get.file_name_ && delimiter_ == get.delimiter_
            && quote_ == get.quote_ && escape_ == get.escape_);
}

auto LogicalExternalFileGet::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
    return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
auto LogicalQueryDerivedGet::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalQueryDerivedGet(*this);
}

auto LogicalQueryDerivedGet::Make(
    parser::AliasType                                                                           table_alias,
    std::unordered_map<parser::AliasType, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map)
    -> Operator {
    auto *get = new LogicalQueryDerivedGet();
    get->table_alias_ = std::move(table_alias);
    get->alias_to_expr_map_ = std::move(alias_to_expr_map);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

auto LogicalQueryDerivedGet::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALQUERYDERIVEDGET) {
        return false;
    }
    const LogicalQueryDerivedGet &node = *static_cast<const LogicalQueryDerivedGet *>(&r);
    if (table_alias_ != node.table_alias_) {
        return false;
    }
    return alias_to_expr_map_ == node.alias_to_expr_map_;
}

auto LogicalQueryDerivedGet::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, std::hash<parser::AliasType>{}(table_alias_));
    for (auto &iter : alias_to_expr_map_) {
        hash = common::HashUtil::CombineHashes(hash, std::hash<parser::AliasType>{}(iter.first));
        hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(iter.second));
    }
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalFilter
//===--------------------------------------------------------------------===//
auto LogicalFilter::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalFilter(*this);
}

auto LogicalFilter::Make(std::vector<AnnotatedExpression> &&predicates) -> Operator {
    auto *op = new LogicalFilter();
    op->predicates_ = std::move(predicates);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalFilter::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALFILTER) {
        return false;
    }
    const LogicalFilter &node = *static_cast<const LogicalFilter *>(&r);

    // This is technically incorrect because the predicates
    // are supposed to be unsorted and this equals check is
    // comparing values at each offset.
    return (predicates_ == node.predicates_);
}

auto LogicalFilter::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    // Again, I think that this is wrong because the value of the hash is based
    // on the location order of the expressions.
    for (auto &pred : predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalProjection
//===--------------------------------------------------------------------===//
auto LogicalProjection::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalProjection(*this);
}

auto LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&expressions)
    -> Operator {
    auto *op = new LogicalProjection();
    op->expressions_ = std::move(expressions);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalProjection::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALPROJECTION) {
        return false;
    }
    const LogicalProjection &node = *static_cast<const LogicalProjection *>(&r);
    for (size_t i = 0; i < expressions_.size(); i++) {
        if (*(expressions_[i]) != *(node.expressions_[i])) {
            return false;
        }
    }
    return true;
}

auto LogicalProjection::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &expr : expressions_) {
        hash = common::HashUtil::SumHashes(hash, expr->Hash());
    }
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalInsert
//===--------------------------------------------------------------------===//
auto LogicalInsert::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalInsert(*this);
}

auto LogicalInsert::Make(
    catalog::db_oid_t                                                                                    database_oid,
    catalog::table_oid_t                                                                                 table_oid,
    std::vector<catalog::col_oid_t>                                                                    &&columns,
    common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> values)
    -> Operator {
#ifndef NDEBUG
    // We need to check whether the number of values for each insert vector
    // matches the number of columns
    for (const auto &insert_vals : *values) {
        NOISEPAGE_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
    }
#endif

    auto *op = new LogicalInsert();
    op->database_oid_ = database_oid;
    op->table_oid_ = table_oid;
    op->columns_ = std::move(columns);
    op->values_ = values;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalInsert::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());

    // Perform a deep hash of the values
    for (const auto &insert_vals : *values_) {
        hash = common::HashUtil::CombineHashInRange(hash, insert_vals.begin(), insert_vals.end());
    }

    return hash;
}

auto LogicalInsert::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALINSERT) {
        return false;
    }
    const LogicalInsert &node = *dynamic_cast<const LogicalInsert *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (columns_ != node.columns_) {
        return false;
    }
    if (values_ != node.values_) {
        return false;
    }
    return (true);
}

//===--------------------------------------------------------------------===//
// LogicalInsertSelect
//===--------------------------------------------------------------------===//
auto LogicalInsertSelect::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalInsertSelect(*this);
}

auto LogicalInsertSelect::Make(catalog::db_oid_t                 database_oid,
                               catalog::table_oid_t              table_oid,
                               std::vector<catalog::col_oid_t> &&columns) -> Operator {
    auto *op = new LogicalInsertSelect();
    op->database_oid_ = database_oid;
    op->table_oid_ = table_oid;
    op->columns_ = std::move(columns);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalInsertSelect::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    return hash;
}

auto LogicalInsertSelect::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALINSERTSELECT) {
        return false;
    }
    const LogicalInsertSelect &node = *dynamic_cast<const LogicalInsertSelect *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    return (true);
}

//===--------------------------------------------------------------------===//
// LogicalLimit
//===--------------------------------------------------------------------===//
auto LogicalLimit::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalLimit(*this);
}

auto LogicalLimit::Make(size_t                                                            offset,
                        size_t                                                            limit,
                        std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_exprs,
                        std::vector<optimizer::OrderByOrderingType>                     &&sort_directions) -> Operator {
    NOISEPAGE_ASSERT(sort_exprs.size() == sort_directions.size(), "Mismatched ORDER BY expressions + directions");
    auto *op = new LogicalLimit();
    op->offset_ = offset;
    op->limit_ = limit;
    op->sort_exprs_ = sort_exprs;
    op->sort_directions_ = std::move(sort_directions);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalLimit::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALLIMIT) {
        return false;
    }
    const LogicalLimit &node = *static_cast<const LogicalLimit *>(&r);
    if (offset_ != node.offset_) {
        return false;
    }
    if (limit_ != node.limit_) {
        return false;
    }
    if (sort_exprs_ != node.sort_exprs_) {
        return false;
    }
    if (sort_directions_ != node.sort_directions_) {
        return false;
    }
    return (true);
}

auto LogicalLimit::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
    hash = common::HashUtil::CombineHashInRange(hash, sort_exprs_.begin(), sort_exprs_.end());
    hash = common::HashUtil::CombineHashInRange(hash, sort_directions_.begin(), sort_directions_.end());
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalDelete
//===--------------------------------------------------------------------===//
auto LogicalDelete::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDelete(*this);
}

auto LogicalDelete::Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid)
    -> Operator {
    auto *op = new LogicalDelete();
    op->database_oid_ = database_oid;
    op->table_alias_ = std::move(table_alias);
    op->table_oid_ = table_oid;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDelete::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    return hash;
}

auto LogicalDelete::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDELETE) {
        return false;
    }
    const LogicalDelete &node = *dynamic_cast<const LogicalDelete *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    return (true);
}

//===--------------------------------------------------------------------===//
// LogicalUpdate
//===--------------------------------------------------------------------===//
auto LogicalUpdate::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalUpdate(*this);
}

auto LogicalUpdate::Make(catalog::db_oid_t                                           database_oid,
                         std::string                                                 table_alias,
                         catalog::table_oid_t                                        table_oid,
                         std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) -> Operator {
    auto *op = new LogicalUpdate();
    op->database_oid_ = database_oid;
    op->table_alias_ = std::move(table_alias);
    op->table_oid_ = table_oid;
    op->updates_ = std::move(updates);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalUpdate::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    for (const auto &clause : updates_) {
        hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetUpdateValue()));
        hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetColumnName()));
    }
    return hash;
}

auto LogicalUpdate::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALUPDATE) {
        return false;
    }
    const LogicalUpdate &node = *dynamic_cast<const LogicalUpdate *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (updates_.size() != node.updates_.size()) {
        return false;
    }
    for (size_t i = 0; i < updates_.size(); i++) {
        if (*(updates_[i]) != *(node.updates_[i])) {
            return false;
        }
    }
    return table_alias_ == node.table_alias_;
}

//===--------------------------------------------------------------------===//
// LogicalExportExternalFile
//===--------------------------------------------------------------------===//
auto LogicalExportExternalFile::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalExportExternalFile(*this);
}

auto LogicalExportExternalFile::Make(parser::ExternalFileFormat format,
                                     std::string                file_name,
                                     char                       delimiter,
                                     char                       quote,
                                     char                       escape) -> Operator {
    auto *op = new LogicalExportExternalFile();
    op->format_ = format;
    op->file_name_ = std::move(file_name);
    op->delimiter_ = delimiter;
    op->quote_ = quote;
    op->escape_ = escape;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalExportExternalFile::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
    return hash;
}

auto LogicalExportExternalFile::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALEXPORTEXTERNALFILE) {
        return false;
    }
    const LogicalExportExternalFile &node = *dynamic_cast<const LogicalExportExternalFile *>(&r);
    if (format_ != node.format_) {
        return false;
    }
    if (file_name_ != node.file_name_) {
        return false;
    }
    if (delimiter_ != node.delimiter_) {
        return false;
    }
    if (quote_ != node.quote_) {
        return false;
    }
    if (escape_ != node.escape_) {
        return false;
    }
    return (true);
}

//===--------------------------------------------------------------------===//
// Logical Dependent Join
//===--------------------------------------------------------------------===//
auto LogicalDependentJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDependentJoin(*this);
}

auto LogicalDependentJoin::Make() -> Operator {
    auto *join = new LogicalDependentJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalDependentJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalDependentJoin();
    join->join_predicates_ = std::move(join_predicates);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalDependentJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalDependentJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDEPENDENTJOIN) {
        return false;
    }
    const LogicalDependentJoin &node = *static_cast<const LogicalDependentJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
auto LogicalMarkJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalMarkJoin(*this);
}

auto LogicalMarkJoin::Make() -> Operator {
    auto *join = new LogicalMarkJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalMarkJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalMarkJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalMarkJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalMarkJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALMARKJOIN) {
        return false;
    }
    const LogicalMarkJoin &node = *static_cast<const LogicalMarkJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
auto LogicalSingleJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalSingleJoin(*this);
}

auto LogicalSingleJoin::Make() -> Operator {
    auto *join = new LogicalSingleJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalSingleJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalSingleJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalSingleJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalSingleJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALSINGLEJOIN) {
        return false;
    }
    const LogicalSingleJoin &node = *static_cast<const LogicalSingleJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
auto LogicalInnerJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalInnerJoin(*this);
}

auto LogicalInnerJoin::Make() -> Operator {
    auto *join = new LogicalInnerJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalInnerJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalInnerJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalInnerJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalInnerJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALINNERJOIN) {
        return false;
    }
    const LogicalInnerJoin &node = *static_cast<const LogicalInnerJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
auto LogicalLeftJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalLeftJoin(*this);
}

auto LogicalLeftJoin::Make() -> Operator {
    auto *join = new LogicalLeftJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalLeftJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalLeftJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalLeftJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalLeftJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALLEFTJOIN) {
        return false;
    }
    const LogicalLeftJoin &node = *static_cast<const LogicalLeftJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
auto LogicalRightJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalRightJoin(*this);
}

auto LogicalRightJoin::Make() -> Operator {
    auto *join = new LogicalRightJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalRightJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalRightJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalRightJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalRightJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALRIGHTJOIN) {
        return false;
    }
    const LogicalRightJoin &node = *static_cast<const LogicalRightJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
auto LogicalOuterJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalOuterJoin(*this);
}

auto LogicalOuterJoin::Make() -> Operator {
    auto *join = new LogicalOuterJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalOuterJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalOuterJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalOuterJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalOuterJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALOUTERJOIN) {
        return false;
    }
    const LogicalOuterJoin &node = *static_cast<const LogicalOuterJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
auto LogicalSemiJoin::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalSemiJoin(*this);
}

auto LogicalSemiJoin::Make() -> Operator {
    auto *join = new LogicalSemiJoin();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalSemiJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) -> Operator {
    auto *join = new LogicalSemiJoin();
    join->join_predicates_ = join_predicates;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

auto LogicalSemiJoin::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : join_predicates_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    return hash;
}

auto LogicalSemiJoin::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALSEMIJOIN) {
        return false;
    }
    const LogicalSemiJoin &node = *static_cast<const LogicalSemiJoin *>(&r);
    return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
auto LogicalAggregateAndGroupBy::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalAggregateAndGroupBy(*this);
}

auto LogicalAggregateAndGroupBy::Make() -> Operator {
    auto *group_by = new LogicalAggregateAndGroupBy();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}

auto LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns)
    -> Operator {
    auto *group_by = new LogicalAggregateAndGroupBy();
    group_by->columns_ = std::move(columns);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}

auto LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                                      std::vector<AnnotatedExpression> &&having) -> Operator {
    auto *group_by = new LogicalAggregateAndGroupBy();
    group_by->columns_ = std::move(columns);
    group_by->having_ = std::move(having);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}
auto LogicalAggregateAndGroupBy::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALAGGREGATEANDGROUPBY) {
        return false;
    }
    const LogicalAggregateAndGroupBy &node = *static_cast<const LogicalAggregateAndGroupBy *>(&r);
    if (having_.size() != node.having_.size() || columns_.size() != node.columns_.size()) {
        return false;
    }
    for (size_t i = 0; i < having_.size(); i++) {
        if (having_[i] != node.having_[i]) {
            return false;
        }
    }
    // originally throw all entries from each vector to one set,
    // and compare if the 2 sets are equal
    // this solves the problem of order of expression within the same hierarchy
    // however causing shared_ptrs to be compared by their memory location to which they points to
    // rather than the content of the object they point to.

    //  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
    //  for (auto &expr : columns_) l_set.emplace(expr);
    //  for (auto &expr : node.columns_) r_set.emplace(expr);
    //  return l_set == r_set;
    for (size_t i = 0; i < columns_.size(); i++) {
        if (*(columns_[i]) != *(node.columns_[i])) {
            return false;
        }
    }
    return true;
}

auto LogicalAggregateAndGroupBy::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    for (auto &pred : having_) {
        auto expr = pred.GetExpr();
        if (expr) {
            hash = common::HashUtil::SumHashes(hash, expr->Hash());
        } else {
            hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
        }
    }
    for (auto &expr : columns_) {
        hash = common::HashUtil::SumHashes(hash, expr->Hash());
    }
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalCreateDatabase
//===--------------------------------------------------------------------===//
auto LogicalCreateDatabase::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateDatabase(*this);
}

auto LogicalCreateDatabase::Make(std::string database_name) -> Operator {
    auto *op = new LogicalCreateDatabase();
    op->database_name_ = std::move(database_name);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateDatabase::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));
    return hash;
}

auto LogicalCreateDatabase::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATEDATABASE) {
        return false;
    }
    const LogicalCreateDatabase &node = *dynamic_cast<const LogicalCreateDatabase *>(&r);
    return node.database_name_ == database_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateFunction
//===--------------------------------------------------------------------===//
auto LogicalCreateFunction::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateFunction(*this);
}

auto LogicalCreateFunction::Make(catalog::db_oid_t                                      database_oid,
                                 catalog::namespace_oid_t                               namespace_oid,
                                 std::string                                            function_name,
                                 parser::PLType                                         language,
                                 std::vector<std::string>                             &&function_body,
                                 std::vector<std::string>                             &&function_param_names,
                                 std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
                                 parser::BaseFunctionParameter::DataType                return_type,
                                 size_t                                                 param_count,
                                 bool                                                   replace) -> Operator {
    auto *op = new LogicalCreateFunction();
    NOISEPAGE_ASSERT(function_param_names.size() == param_count && function_param_types.size() == param_count,
                     "Mismatched number of items in vector and number of function parameters");
    op->database_oid_ = database_oid;
    op->namespace_oid_ = namespace_oid;
    op->function_name_ = std::move(function_name);
    op->function_body_ = std::move(function_body);
    op->function_param_names_ = std::move(function_param_names);
    op->function_param_types_ = std::move(function_param_types);
    op->is_replace_ = replace;
    op->param_count_ = param_count;
    op->return_type_ = return_type;
    op->language_ = language;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateFunction::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(param_count_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_replace_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(return_type_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(language_));
    hash = common::HashUtil::CombineHashInRange(hash, function_body_.begin(), function_body_.end());
    hash = common::HashUtil::CombineHashInRange(hash, function_param_names_.begin(), function_param_names_.end());
    hash = common::HashUtil::CombineHashInRange(hash, function_param_types_.begin(), function_param_types_.end());
    return hash;
}

auto LogicalCreateFunction::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATEFUNCTION) {
        return false;
    }
    const LogicalCreateFunction &node = *dynamic_cast<const LogicalCreateFunction *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (namespace_oid_ != node.namespace_oid_) {
        return false;
    }
    if (function_name_ != node.function_name_) {
        return false;
    }
    if (function_body_ != node.function_body_) {
        return false;
    }
    if (param_count_ != node.param_count_) {
        return false;
    }
    if (return_type_ != node.return_type_) {
        return false;
    }
    if (function_param_types_ != node.function_param_types_) {
        return false;
    }
    if (function_param_names_ != node.function_param_names_) {
        return false;
    }
    if (is_replace_ != node.is_replace_) {
        return false;
    }
    return language_ == node.language_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateIndex
//===--------------------------------------------------------------------===//
auto LogicalCreateIndex::Copy() const -> BaseOperatorNodeContents * {
    auto *op = new LogicalCreateIndex();
    op->database_oid_ = database_oid_;
    op->namespace_oid_ = namespace_oid_;
    op->table_oid_ = table_oid_;
    op->index_type_ = index_type_;
    op->unique_index_ = unique_index_;
    op->index_name_ = index_name_;
    op->index_attrs_ = index_attrs_;
    op->index_options_ = catalog::IndexOptions(index_options_);
    return op;
}

auto LogicalCreateIndex::Make(catalog::db_oid_t                                               database_oid,
                              catalog::namespace_oid_t                                        namespace_oid,
                              catalog::table_oid_t                                            table_oid,
                              parser::IndexType                                               index_type,
                              bool                                                            unique,
                              std::string                                                     index_name,
                              std::vector<common::ManagedPointer<parser::AbstractExpression>> index_attrs,
                              catalog::IndexOptions index_options) -> Operator {
    auto *op = new LogicalCreateIndex();
    op->database_oid_ = database_oid;
    op->namespace_oid_ = namespace_oid;
    op->table_oid_ = table_oid;
    op->index_type_ = index_type;
    op->unique_index_ = unique;
    op->index_name_ = std::move(index_name);
    op->index_attrs_ = std::move(index_attrs);
    op->index_options_ = std::move(index_options);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateIndex::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_type_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(unique_index_));
    for (const auto &attr : index_attrs_) {
        hash = common::HashUtil::CombineHashes(hash, attr->Hash());
    }
    hash = common::HashUtil::CombineHashes(hash, index_options_.Hash());
    return hash;
}

auto LogicalCreateIndex::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATEINDEX) {
        return false;
    }
    const LogicalCreateIndex &node = *dynamic_cast<const LogicalCreateIndex *>(&r);
    if (namespace_oid_ != node.namespace_oid_) {
        return false;
    }
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (index_type_ != node.index_type_) {
        return false;
    }
    if (index_name_ != node.index_name_) {
        return false;
    }
    if (unique_index_ != node.unique_index_) {
        return false;
    }
    if (index_attrs_.size() != node.index_attrs_.size()) {
        return false;
    }
    for (size_t i = 0; i < index_attrs_.size(); i++) {
        if (*(index_attrs_[i]) != *(node.index_attrs_[i])) {
            return false;
        }
    }
    return index_options_ == node.index_options_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateTable
//===--------------------------------------------------------------------===//
auto LogicalCreateTable::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateTable(*this);
}

auto LogicalCreateTable::Make(catalog::namespace_oid_t                                        namespace_oid,
                              std::string                                                     table_name,
                              std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&columns,
                              std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys)
    -> Operator {
    auto *op = new LogicalCreateTable();
    op->namespace_oid_ = namespace_oid;
    op->table_name_ = std::move(table_name);
    op->columns_ = std::move(columns);
    op->foreign_keys_ = std::move(foreign_keys);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateTable::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
    hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
    for (const auto &col : columns_) {
        hash = common::HashUtil::CombineHashes(hash, col->Hash());
    }
    for (const auto &fk : foreign_keys_) {
        hash = common::HashUtil::CombineHashes(hash, fk->Hash());
    }
    return hash;
}

auto LogicalCreateTable::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATETABLE) {
        return false;
    }
    const LogicalCreateTable &node = *dynamic_cast<const LogicalCreateTable *>(&r);
    if (namespace_oid_ != node.namespace_oid_) {
        return false;
    }
    if (table_name_ != node.table_name_) {
        return false;
    }
    if (columns_.size() != node.columns_.size()) {
        return false;
    }
    for (size_t i = 0; i < columns_.size(); i++) {
        if (*(columns_[i]) != *(node.columns_[i])) {
            return false;
        }
    }
    if (foreign_keys_.size() != node.foreign_keys_.size()) {
        return false;
    }
    for (size_t i = 0; i < foreign_keys_.size(); i++) {
        if (*(foreign_keys_[i]) != *(node.foreign_keys_[i])) {
            return false;
        }
    }
    return true;
}

//===--------------------------------------------------------------------===//
// LogicalCreateNamespace
//===--------------------------------------------------------------------===//
auto LogicalCreateNamespace::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateNamespace(*this);
}

auto LogicalCreateNamespace::Make(std::string namespace_name) -> Operator {
    auto *op = new LogicalCreateNamespace();
    op->namespace_name_ = std::move(namespace_name);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateNamespace::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
    return hash;
}

auto LogicalCreateNamespace::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATENAMESPACE) {
        return false;
    }
    const LogicalCreateNamespace &node = *dynamic_cast<const LogicalCreateNamespace *>(&r);
    return node.namespace_name_ == namespace_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateTrigger
//===--------------------------------------------------------------------===//
auto LogicalCreateTrigger::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateTrigger(*this);
}

auto LogicalCreateTrigger::Make(catalog::db_oid_t                                    database_oid,
                                catalog::namespace_oid_t                             namespace_oid,
                                catalog::table_oid_t                                 table_oid,
                                std::string                                          trigger_name,
                                std::vector<std::string>                           &&trigger_funcnames,
                                std::vector<std::string>                           &&trigger_args,
                                std::vector<catalog::col_oid_t>                    &&trigger_columns,
                                common::ManagedPointer<parser::AbstractExpression> &&trigger_when,
                                int16_t                                              trigger_type) -> Operator {
    auto *op = new LogicalCreateTrigger();
    op->database_oid_ = database_oid;
    op->namespace_oid_ = namespace_oid;
    op->table_oid_ = table_oid;
    op->trigger_name_ = std::move(trigger_name);
    op->trigger_funcnames_ = std::move(trigger_funcnames);
    op->trigger_args_ = std::move(trigger_args);
    op->trigger_columns_ = std::move(trigger_columns);
    op->trigger_when_ = trigger_when;
    op->trigger_type_ = trigger_type;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateTrigger::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_type_));
    hash = common::HashUtil::CombineHashes(hash, trigger_when_->Hash());
    hash = common::HashUtil::CombineHashInRange(hash, trigger_funcnames_.begin(), trigger_funcnames_.end());
    hash = common::HashUtil::CombineHashInRange(hash, trigger_args_.begin(), trigger_args_.end());
    hash = common::HashUtil::CombineHashInRange(hash, trigger_columns_.begin(), trigger_columns_.end());
    return hash;
}

auto LogicalCreateTrigger::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATETRIGGER) {
        return false;
    }
    const LogicalCreateTrigger &node = *dynamic_cast<const LogicalCreateTrigger *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (namespace_oid_ != node.namespace_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (trigger_name_ != node.trigger_name_) {
        return false;
    }
    if (trigger_funcnames_ != node.trigger_funcnames_) {
        return false;
    }
    if (trigger_args_ != node.trigger_args_) {
        return false;
    }
    if (trigger_columns_ != node.trigger_columns_) {
        return false;
    }
    if (trigger_type_ != node.trigger_type_) {
        return false;
    }
    if (trigger_when_ == nullptr) {
        return node.trigger_when_ == nullptr;
    }
    return node.trigger_when_ != nullptr && *trigger_when_ == *node.trigger_when_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateView
//===--------------------------------------------------------------------===//
auto LogicalCreateView::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCreateView(*this);
}

auto LogicalCreateView::Make(catalog::db_oid_t                               database_oid,
                             catalog::namespace_oid_t                        namespace_oid,
                             std::string                                     view_name,
                             common::ManagedPointer<parser::SelectStatement> view_query) -> Operator {
    auto *op = new LogicalCreateView();
    op->database_oid_ = database_oid;
    op->namespace_oid_ = namespace_oid;
    op->view_name_ = std::move(view_name);
    op->view_query_ = view_query;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCreateView::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));
    if (view_query_ != nullptr) {
        hash = common::HashUtil::CombineHashes(hash, view_query_->Hash());
    }
    return hash;
}

auto LogicalCreateView::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCREATEVIEW) {
        return false;
    }
    const LogicalCreateView &node = *dynamic_cast<const LogicalCreateView *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (namespace_oid_ != node.namespace_oid_) {
        return false;
    }
    if (view_name_ != node.view_name_) {
        return false;
    }
    if (view_query_ == nullptr) {
        return node.view_query_ == nullptr;
    }
    return node.view_query_ != nullptr && *view_query_ == *node.view_query_;
}

//===--------------------------------------------------------------------===//
// LogicalDropDatabase
//===--------------------------------------------------------------------===//
auto LogicalDropDatabase::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropDatabase(*this);
}

auto LogicalDropDatabase::Make(catalog::db_oid_t db_oid) -> Operator {
    auto *op = new LogicalDropDatabase();
    op->db_oid_ = db_oid;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropDatabase::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(db_oid_));
    return hash;
}

auto LogicalDropDatabase::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPDATABASE) {
        return false;
    }
    const LogicalDropDatabase &node = *dynamic_cast<const LogicalDropDatabase *>(&r);
    return node.db_oid_ == db_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropTable
//===--------------------------------------------------------------------===//
auto LogicalDropTable::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropTable(*this);
}

auto LogicalDropTable::Make(catalog::table_oid_t table_oid) -> Operator {
    auto *op = new LogicalDropTable();
    op->table_oid_ = table_oid;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropTable::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    return hash;
}

auto LogicalDropTable::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPTABLE) {
        return false;
    }
    const LogicalDropTable &node = *dynamic_cast<const LogicalDropTable *>(&r);
    return node.table_oid_ == table_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropIndex
//===--------------------------------------------------------------------===//
auto LogicalDropIndex::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropIndex(*this);
}

auto LogicalDropIndex::Make(catalog::index_oid_t index_oid) -> Operator {
    auto *op = new LogicalDropIndex();
    op->index_oid_ = index_oid;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropIndex::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));
    return hash;
}

auto LogicalDropIndex::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPINDEX) {
        return false;
    }
    const LogicalDropIndex &node = *dynamic_cast<const LogicalDropIndex *>(&r);
    return node.index_oid_ == index_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropNamespace
//===--------------------------------------------------------------------===//
auto LogicalDropNamespace::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropNamespace(*this);
}

auto LogicalDropNamespace::Make(catalog::namespace_oid_t namespace_oid) -> Operator {
    auto *op = new LogicalDropNamespace();
    op->namespace_oid_ = namespace_oid;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropNamespace::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
    return hash;
}

auto LogicalDropNamespace::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPNAMESPACE) {
        return false;
    }
    const LogicalDropNamespace &node = *dynamic_cast<const LogicalDropNamespace *>(&r);
    return node.namespace_oid_ == namespace_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropTrigger
//===--------------------------------------------------------------------===//
auto LogicalDropTrigger::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropTrigger(*this);
}

auto LogicalDropTrigger::Make(catalog::db_oid_t database_oid, catalog::trigger_oid_t trigger_oid, bool if_exists)
    -> Operator {
    auto *op = new LogicalDropTrigger();
    op->database_oid_ = database_oid;
    op->trigger_oid_ = trigger_oid;
    op->if_exists_ = if_exists;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropTrigger::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
    return hash;
}

auto LogicalDropTrigger::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPTRIGGER) {
        return false;
    }
    const LogicalDropTrigger &node = *dynamic_cast<const LogicalDropTrigger *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (trigger_oid_ != node.trigger_oid_) {
        return false;
    }
    return if_exists_ == node.if_exists_;
}

//===--------------------------------------------------------------------===//
// LogicalDropView
//===--------------------------------------------------------------------===//
auto LogicalDropView::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalDropView(*this);
}

auto LogicalDropView::Make(catalog::db_oid_t database_oid, catalog::view_oid_t view_oid, bool if_exists) -> Operator {
    auto *op = new LogicalDropView();
    op->database_oid_ = database_oid;
    op->view_oid_ = view_oid;
    op->if_exists_ = if_exists;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalDropView::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
    return hash;
}

auto LogicalDropView::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALDROPVIEW) {
        return false;
    }
    const LogicalDropView &node = *dynamic_cast<const LogicalDropView *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (view_oid_ != node.view_oid_) {
        return false;
    }
    return if_exists_ == node.if_exists_;
}

//===--------------------------------------------------------------------===//
// LogicalAnalyze
//===--------------------------------------------------------------------===//
auto LogicalAnalyze::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalAnalyze(*this);
}

auto LogicalAnalyze::Make(catalog::db_oid_t                 database_oid,
                          catalog::table_oid_t              table_oid,
                          std::vector<catalog::col_oid_t> &&columns) -> Operator {
    auto *op = new LogicalAnalyze();
    op->database_oid_ = database_oid;
    op->table_oid_ = table_oid;
    op->columns_ = std::move(columns);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalAnalyze::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
    return hash;
}

auto LogicalAnalyze::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALANALYZE) {
        return false;
    }
    const LogicalAnalyze &node = *dynamic_cast<const LogicalAnalyze *>(&r);
    if (database_oid_ != node.database_oid_) {
        return false;
    }
    if (table_oid_ != node.table_oid_) {
        return false;
    }
    if (columns_ != node.columns_) {
        return false;
    }
    return true;
}

//===--------------------------------------------------------------------===//
// LogicalUnion
//===--------------------------------------------------------------------===//
auto LogicalUnion::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalUnion(*this);
}

auto LogicalUnion::Make(bool                                            is_all,
                        common::ManagedPointer<parser::SelectStatement> left_expr,
                        common::ManagedPointer<parser::SelectStatement> right_expr) -> Operator {
    auto *op = new LogicalUnion();
    op->left_expr_ = left_expr;
    op->right_expr_ = right_expr;
    op->is_all_ = is_all;
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalUnion::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALUNION) {
        return false;
    }
    const LogicalUnion &node = *dynamic_cast<const LogicalUnion *>(&r);
    return node.right_expr_ == right_expr_ && node.left_expr_ == left_expr_ && node.is_all_ == is_all_;
}

auto LogicalUnion::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, left_expr_->Hash());
    hash = common::HashUtil::CombineHashes(hash, right_expr_->Hash());
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_all_));
    return hash;
}

//===--------------------------------------------------------------------===//
// LogicalCteScan
//===--------------------------------------------------------------------===//
auto LogicalCteScan::Copy() const -> BaseOperatorNodeContents * {
    return new LogicalCteScan(*this);
}

auto LogicalCteScan::Make() -> Operator {
    auto *op = new LogicalCteScan();
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCteScan::Make(
    std::string                                                                  table_alias,
    std::string                                                                  table_name,
    catalog::table_oid_t                                                         table_oid,
    catalog::Schema                                                              table_schema,
    std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> child_expressions,
    parser::CteType                                                              cte_type,
    std::vector<AnnotatedExpression>                                           &&scan_predicate) -> Operator {
    auto *op = new LogicalCteScan();
    op->table_schema_ = std::move(table_schema);
    op->table_alias_ = std::move(table_alias);
    op->table_name_ = std::move(table_name);
    op->table_oid_ = table_oid;
    op->child_expressions_ = std::move(child_expressions);
    op->cte_type_ = cte_type;
    op->scan_predicate_ = std::move(scan_predicate);
    return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

auto LogicalCteScan::operator==(const BaseOperatorNodeContents &r) -> bool {
    if (r.GetOpType() != OpType::LOGICALCTESCAN) {
        return false;
    }
    const LogicalCteScan &node = *dynamic_cast<const LogicalCteScan *>(&r);
    bool                  ret = (table_alias_ == node.table_alias_ && cte_type_ == node.cte_type_);
    if (scan_predicate_.size() != node.scan_predicate_.size()) {
        return false;
    }
    for (size_t i = 0; i < scan_predicate_.size(); i++) {
        if (scan_predicate_[i].GetExpr() != node.scan_predicate_[i].GetExpr()) {
            return false;
        }
    }
    return ret;
}

auto LogicalCteScan::Hash() const -> common::hash_t {
    common::hash_t hash = BaseOperatorNodeContents::Hash();
    hash = common::HashUtil::CombineHashes(hash, static_cast<uint32_t>(cte_type_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
    hash = common::HashUtil::CombineHashInRange(hash, scan_predicate_.begin(), scan_predicate_.end());
    return hash;
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNodeContents<LeafOperator>::name = "LeafOperator";
template <>
const char *OperatorNodeContents<LogicalGet>::name = "LogicalGet";
template <>
const char *OperatorNodeContents<LogicalExternalFileGet>::name = "LogicalExternalFileGet";
template <>
const char *OperatorNodeContents<LogicalQueryDerivedGet>::name = "LogicalQueryDerivedGet";
template <>
const char *OperatorNodeContents<LogicalFilter>::name = "LogicalFilter";
template <>
const char *OperatorNodeContents<LogicalProjection>::name = "LogicalProjection";
template <>
const char *OperatorNodeContents<LogicalMarkJoin>::name = "LogicalMarkJoin";
template <>
const char *OperatorNodeContents<LogicalSingleJoin>::name = "LogicalSingleJoin";
template <>
const char *OperatorNodeContents<LogicalDependentJoin>::name = "LogicalDependentJoin";
template <>
const char *OperatorNodeContents<LogicalInnerJoin>::name = "LogicalInnerJoin";
template <>
const char *OperatorNodeContents<LogicalLeftJoin>::name = "LogicalLeftJoin";
template <>
const char *OperatorNodeContents<LogicalRightJoin>::name = "LogicalRightJoin";
template <>
const char *OperatorNodeContents<LogicalOuterJoin>::name = "LogicalOuterJoin";
template <>
const char *OperatorNodeContents<LogicalSemiJoin>::name = "LogicalSemiJoin";
template <>
const char *OperatorNodeContents<LogicalAggregateAndGroupBy>::name = "LogicalAggregateAndGroupBy";
template <>
const char *OperatorNodeContents<LogicalInsert>::name = "LogicalInsert";
template <>
const char *OperatorNodeContents<LogicalInsertSelect>::name = "LogicalInsertSelect";
template <>
const char *OperatorNodeContents<LogicalUpdate>::name = "LogicalUpdate";
template <>
const char *OperatorNodeContents<LogicalDelete>::name = "LogicalDelete";
template <>
const char *OperatorNodeContents<LogicalLimit>::name = "LogicalLimit";
template <>
const char *OperatorNodeContents<LogicalExportExternalFile>::name = "LogicalExportExternalFile";
template <>
const char *OperatorNodeContents<LogicalCreateDatabase>::name = "LogicalCreateDatabase";
template <>
const char *OperatorNodeContents<LogicalCreateTable>::name = "LogicalCreateTable";
template <>
const char *OperatorNodeContents<LogicalCreateIndex>::name = "LogicalCreateIndex";
template <>
const char *OperatorNodeContents<LogicalCreateFunction>::name = "LogicalCreateFunction";
template <>
const char *OperatorNodeContents<LogicalCreateNamespace>::name = "LogicalCreateNamespace";
template <>
const char *OperatorNodeContents<LogicalCreateTrigger>::name = "LogicalCreateTrigger";
template <>
const char *OperatorNodeContents<LogicalCreateView>::name = "LogicalCreateView";
template <>
const char *OperatorNodeContents<LogicalDropDatabase>::name = "LogicalDropDatabase";
template <>
const char *OperatorNodeContents<LogicalDropTable>::name = "LogicalDropTable";
template <>
const char *OperatorNodeContents<LogicalDropIndex>::name = "LogicalDropIndex";
template <>
const char *OperatorNodeContents<LogicalDropNamespace>::name = "LogicalDropNamespace";
template <>
const char *OperatorNodeContents<LogicalDropTrigger>::name = "LogicalDropTrigger";
template <>
const char *OperatorNodeContents<LogicalDropView>::name = "LogicalDropView";
template <>
const char *OperatorNodeContents<LogicalAnalyze>::name = "LogicalAnalyze";
template <>
const char *OperatorNodeContents<LogicalCteScan>::name = "LogicalCteScan";
template <>
const char *OperatorNodeContents<LogicalUnion>::name = "LogicalUnion";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNodeContents<LeafOperator>::type = OpType::LEAF;
template <>
OpType OperatorNodeContents<LogicalGet>::type = OpType::LOGICALGET;
template <>
OpType OperatorNodeContents<LogicalExternalFileGet>::type = OpType::LOGICALEXTERNALFILEGET;
template <>
OpType OperatorNodeContents<LogicalQueryDerivedGet>::type = OpType::LOGICALQUERYDERIVEDGET;
template <>
OpType OperatorNodeContents<LogicalFilter>::type = OpType::LOGICALFILTER;
template <>
OpType OperatorNodeContents<LogicalProjection>::type = OpType::LOGICALPROJECTION;
template <>
OpType OperatorNodeContents<LogicalMarkJoin>::type = OpType::LOGICALMARKJOIN;
template <>
OpType OperatorNodeContents<LogicalSingleJoin>::type = OpType::LOGICALSINGLEJOIN;
template <>
OpType OperatorNodeContents<LogicalDependentJoin>::type = OpType::LOGICALDEPENDENTJOIN;
template <>
OpType OperatorNodeContents<LogicalInnerJoin>::type = OpType::LOGICALINNERJOIN;
template <>
OpType OperatorNodeContents<LogicalLeftJoin>::type = OpType::LOGICALLEFTJOIN;
template <>
OpType OperatorNodeContents<LogicalRightJoin>::type = OpType::LOGICALRIGHTJOIN;
template <>
OpType OperatorNodeContents<LogicalOuterJoin>::type = OpType::LOGICALOUTERJOIN;
template <>
OpType OperatorNodeContents<LogicalSemiJoin>::type = OpType::LOGICALSEMIJOIN;
template <>
OpType OperatorNodeContents<LogicalAggregateAndGroupBy>::type = OpType::LOGICALAGGREGATEANDGROUPBY;
template <>
OpType OperatorNodeContents<LogicalInsert>::type = OpType::LOGICALINSERT;
template <>
OpType OperatorNodeContents<LogicalInsertSelect>::type = OpType::LOGICALINSERTSELECT;
template <>
OpType OperatorNodeContents<LogicalUpdate>::type = OpType::LOGICALUPDATE;
template <>
OpType OperatorNodeContents<LogicalDelete>::type = OpType::LOGICALDELETE;
template <>
OpType OperatorNodeContents<LogicalLimit>::type = OpType::LOGICALLIMIT;
template <>
OpType OperatorNodeContents<LogicalExportExternalFile>::type = OpType::LOGICALEXPORTEXTERNALFILE;
template <>
OpType OperatorNodeContents<LogicalCreateDatabase>::type = OpType::LOGICALCREATEDATABASE;
template <>
OpType OperatorNodeContents<LogicalCreateTable>::type = OpType::LOGICALCREATETABLE;
template <>
OpType OperatorNodeContents<LogicalCreateIndex>::type = OpType::LOGICALCREATEINDEX;
template <>
OpType OperatorNodeContents<LogicalCreateFunction>::type = OpType::LOGICALCREATEFUNCTION;
template <>
OpType OperatorNodeContents<LogicalCreateNamespace>::type = OpType::LOGICALCREATENAMESPACE;
template <>
OpType OperatorNodeContents<LogicalCreateTrigger>::type = OpType::LOGICALCREATETRIGGER;
template <>
OpType OperatorNodeContents<LogicalCreateView>::type = OpType::LOGICALCREATEVIEW;
template <>
OpType OperatorNodeContents<LogicalDropDatabase>::type = OpType::LOGICALDROPDATABASE;
template <>
OpType OperatorNodeContents<LogicalDropTable>::type = OpType::LOGICALDROPTABLE;
template <>
OpType OperatorNodeContents<LogicalDropIndex>::type = OpType::LOGICALDROPINDEX;
template <>
OpType OperatorNodeContents<LogicalDropNamespace>::type = OpType::LOGICALDROPNAMESPACE;
template <>
OpType OperatorNodeContents<LogicalDropTrigger>::type = OpType::LOGICALDROPTRIGGER;
template <>
OpType OperatorNodeContents<LogicalDropView>::type = OpType::LOGICALDROPVIEW;
template <>
OpType OperatorNodeContents<LogicalAnalyze>::type = OpType::LOGICALANALYZE;
template <>
OpType OperatorNodeContents<LogicalCteScan>::type = OpType::LOGICALCTESCAN;
template <>
OpType OperatorNodeContents<LogicalUnion>::type = OpType::LOGICALUNION;

} // namespace noisepage::optimizer
