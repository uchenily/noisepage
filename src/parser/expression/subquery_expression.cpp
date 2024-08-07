#include "parser/expression/subquery_expression.h"

#include "common/json.h"

namespace noisepage::parser {

auto SubqueryExpression::Copy() const -> std::unique_ptr<AbstractExpression> {
    std::vector<common::ManagedPointer<AbstractExpression>> select_columns;
    for (const auto &col : subselect_->GetSelectColumns()) {
        select_columns.emplace_back(common::ManagedPointer(col));
    }

    auto group_by = subselect_->GetSelectGroupBy() == nullptr ? nullptr : subselect_->GetSelectGroupBy()->Copy();
    auto order_by = subselect_->GetSelectOrderBy() == nullptr ? nullptr : subselect_->GetSelectOrderBy()->Copy();
    auto limit = subselect_->GetSelectLimit() == nullptr ? nullptr : subselect_->GetSelectLimit()->Copy();
    auto with = subselect_->GetSelectWith();

    // Make a copy of WITH tables
    std::vector<std::unique_ptr<TableRef>> with_copy{};
    with_copy.reserve(with.size());
    std::transform(with.cbegin(), with.cend(), std::back_inserter(with_copy), [](const auto &w) {
        return w->Copy();
    });

    auto parser_select = std::make_unique<SelectStatement>(std::move(select_columns),
                                                           subselect_->IsSelectDistinct(),
                                                           subselect_->GetSelectTable()->Copy(),
                                                           subselect_->GetSelectCondition(),
                                                           std::move(group_by),
                                                           std::move(order_by),
                                                           std::move(limit),
                                                           std::move(with_copy));
    auto expr = std::make_unique<SubqueryExpression>(std::move(parser_select));
    expr->SetMutableStateForCopy(*this);
    return expr;
}

auto SubqueryExpression::DeriveDepth() -> int {
    int current_depth = this->GetDepth();
    for (auto &select_elem : subselect_->GetSelectColumns()) {
        int select_depth = select_elem->DeriveDepth();
        if (select_depth >= 0 && (current_depth == -1 || select_depth < current_depth)) {
            this->SetDepth(select_depth);
            current_depth = select_depth;
        }
    }
    auto where = subselect_->GetSelectCondition();
    if (where != nullptr) {
        auto where_depth = const_cast<parser::AbstractExpression *>(where.Get())->DeriveDepth();
        if (where_depth >= 0 && where_depth < current_depth) {
            this->SetDepth(where_depth);
        }
    }
    return this->GetDepth();
}

auto SubqueryExpression::Hash() const -> common::hash_t {
    common::hash_t hash = AbstractExpression::Hash();
    for (auto select_elem : subselect_->GetSelectColumns()) {
        hash = common::HashUtil::CombineHashes(hash, select_elem->Hash());
    }

    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(subselect_->IsSelectDistinct()));
    if (subselect_->GetSelectCondition() != nullptr) {
        hash = common::HashUtil::CombineHashes(hash, subselect_->GetSelectCondition()->Hash());
    }
    return hash;
}

auto SubqueryExpression::ToJson() const -> nlohmann::json {
    nlohmann::json j = AbstractExpression::ToJson();
    j["subselect"] = subselect_->ToJson();
    return j;
}

auto SubqueryExpression::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<AbstractExpression>> {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto                                             e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    subselect_ = std::make_unique<parser::SelectStatement>();
    auto e2 = subselect_->FromJson(j.at("subselect"));
    exprs.insert(exprs.end(), std::make_move_iterator(e2.begin()), std::make_move_iterator(e2.end()));
    return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(SubqueryExpression);

} // namespace noisepage::parser
