#include "planner/plannodes/output_schema.h"

#include "common/json.h"

namespace noisepage::planner {

auto OutputSchema::Column::ToJson() const -> nlohmann::json {
    nlohmann::json j;
    j["name"] = name_;
    j["type"] = type_;
    j["expr"] = expr_->ToJson();
    return j;
}

auto OutputSchema::Column::FromJson(const nlohmann::json &j)
    -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    name_ = j.at("name").get<std::string>();
    type_ = j.at("type").get<execution::sql::SqlTypeId>();

    if (!j.at("expr").is_null()) {
        auto deserialized = parser::DeserializeExpression(j.at("expr"));
        expr_ = std::move(deserialized.result_);
        return std::move(deserialized.non_owned_exprs_);
    }

    return {};
}

auto OutputSchema::ToJson() const -> nlohmann::json {
    nlohmann::json              j;
    std::vector<nlohmann::json> columns;
    for (const auto &col : columns_) {
        columns.emplace_back(col.ToJson());
    }
    j["columns"] = columns;
    return j;
}

auto OutputSchema::FromJson(const nlohmann::json &j) -> std::vector<std::unique_ptr<parser::AbstractExpression>> {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;

    std::vector<nlohmann::json> columns_json = j.at("columns");
    for (const auto &j : columns_json) {
        Column c;
        auto   nonowned = c.FromJson(j);
        columns_.emplace_back(std::move(c));
        exprs.insert(exprs.end(), std::make_move_iterator(nonowned.begin()), std::make_move_iterator(nonowned.end()));
    }

    return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(OutputSchema::Column);
DEFINE_JSON_BODY_DECLARATIONS(OutputSchema);

} // namespace noisepage::planner
