#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "common/json_header.h"
#include "expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"

namespace noisepage {
namespace binder {
    class BindNodeVisitor;
}
namespace parser {
    class SelectStatement;
    class TableRef;

    /**
     * Represents a join table.
     */
    class JoinDefinition {
    public:
        /**
         * @param type join type
         * @param left left table
         * @param right right table
         * @param condition join condition
         */
        JoinDefinition(JoinType                                   type,
                       std::unique_ptr<TableRef>                  left,
                       std::unique_ptr<TableRef>                  right,
                       common::ManagedPointer<AbstractExpression> condition)
            : type_(type)
            , left_(std::move(left))
            , right_(std::move(right))
            , condition_(condition) {}

        /**
         * Default constructor used for deserialization
         */
        JoinDefinition() = default;

        /**
         * @return a copy of the join definition
         */
        std::unique_ptr<JoinDefinition> Copy();

        /**
         * @param v Visitor pattern for the JOIN definition
         */
        void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
            v->Visit(common::ManagedPointer(this));
        }

        /**
         * @return type of join
         */
        JoinType GetJoinType() {
            return type_;
        }

        /**
         * @return left table
         */
        common::ManagedPointer<TableRef> GetLeftTable() {
            return common::ManagedPointer(left_);
        }

        /**
         * @return right table
         */
        common::ManagedPointer<TableRef> GetRightTable() {
            return common::ManagedPointer(right_);
        }

        /**
         * @return join condition
         */
        common::ManagedPointer<AbstractExpression> GetJoinCondition() {
            return condition_;
        }

        /**
         * @return the hashed value of this join definition
         */
        common::hash_t Hash() const;

        /**
         * Logical equality check.
         * @param rhs other
         * @return true if the two JoinDefinition are logically equal
         */
        bool operator==(const JoinDefinition &rhs) const;

        /**
         * Logical inequality check.
         * @param rhs other
         * @return true if the two JoinDefinition are logically unequal
         */
        bool operator!=(const JoinDefinition &rhs) const {
            return !(operator==(rhs));
        }

        /**
         * @return JoinDefinition serialized to json
         */
        nlohmann::json ToJson() const;

        /**
         * @param j json to deserialize
         */
        std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

    private:
        JoinType                                   type_;
        std::unique_ptr<TableRef>                  left_;
        std::unique_ptr<TableRef>                  right_;
        common::ManagedPointer<AbstractExpression> condition_;
    };

    DEFINE_JSON_HEADER_DECLARATIONS(JoinDefinition);

    /**
     * Holds references to tables, either via table names or a select statement.
     */
    class TableRef {
    public:
        // TODO(WAN): was and is still a mess

        /**
         * Default constructor used for deserialization
         */
        TableRef() = default;

        /**
         * @return a copy of the table reference
         */
        std::unique_ptr<TableRef> Copy() const;

        /**
         * Construct a table reference.
         * @param alias alias for table ref
         * @param table_info table information to use in creation
         */
        TableRef(AliasType alias, std::unique_ptr<TableInfo> table_info)
            : type_(TableReferenceType::NAME)
            , alias_(std::move(alias))
            , table_info_(std::move(table_info)) {}

        /**
         * Construct a table reference.
         * @param alias alias for table ref
         * @param select select statement to use in creation
         */
        TableRef(AliasType alias, std::unique_ptr<SelectStatement> select)
            : type_(TableReferenceType::SELECT)
            , alias_(std::move(alias))
            , select_(std::move(select)) {}

        /**
         * Construct a table reference.
         * @param alias alias for table ref
         * @param select select statement to use in creation
         * @param cte_col_aliases aliases for the columns
         * @param cte_type The type of the CTE referenced by this table
         */
        TableRef(AliasType                        alias,
                 std::unique_ptr<SelectStatement> select,
                 std::vector<AliasType>           cte_col_aliases,
                 parser::CteType                  cte_type)
            : type_(TableReferenceType::SELECT)
            , alias_(std::move(alias))
            , select_(std::move(select))
            , cte_col_aliases_(std::move(cte_col_aliases))
            , cte_type_(cte_type) {}

        /**
         * Construct a table reference.
         * @param list table refs to use in creation
         */
        explicit TableRef(std::vector<std::unique_ptr<TableRef>> list)
            : type_(TableReferenceType::CROSS_PRODUCT)
            , list_(std::move(list)) {}

        /**
         * Construct a table reference.
         * @param join join definition to use in creation
         */
        explicit TableRef(std::unique_ptr<JoinDefinition> join)
            : type_(TableReferenceType::JOIN)
            , join_(std::move(join)) {}

        /**
         * @param alias alias for table ref
         * @param table_info table info to use in creation
         * @return unique pointer to the created table ref
         */
        static std::unique_ptr<TableRef> CreateTableRefByName(AliasType alias, std::unique_ptr<TableInfo> table_info) {
            return std::make_unique<TableRef>(std::move(alias), std::move(table_info));
        }

        /**
         * @param alias alias for table ref
         * @param select select statement to use in creation
         * @return unique pointer to the created table ref
         */
        static std::unique_ptr<TableRef> CreateTableRefBySelect(AliasType                        alias,
                                                                std::unique_ptr<SelectStatement> select) {
            return std::make_unique<TableRef>(std::move(alias), std::move(select));
        }

        /**
         * @param alias alias for table ref
         * @param select select statement to use in creation
         * @param cte_col_aliases aliases for column names
         * @param cte_type the type of CTE
         * @return unique pointer to the created (CTE) table ref
         */
        static std::unique_ptr<TableRef> CreateCTETableRefBySelect(std::string                      alias,
                                                                   std::unique_ptr<SelectStatement> select,
                                                                   std::vector<AliasType>           cte_col_aliases,
                                                                   parser::CteType                  cte_type) {
            return std::make_unique<TableRef>(parser::AliasType(std::move(alias)),
                                              std::move(select),
                                              std::move(cte_col_aliases),
                                              cte_type);
        }

        /**
         * @param list table refs to use in creation
         * @return unique pointer to the created table ref
         */
        static std::unique_ptr<TableRef> CreateTableRefByList(std::vector<std::unique_ptr<TableRef>> list) {
            return std::make_unique<TableRef>(std::move(list));
        }

        /**
         * @param join join definition to use in creation
         * @return unique pointer to the created table ref
         */
        static std::unique_ptr<TableRef> CreateTableRefByJoin(std::unique_ptr<JoinDefinition> join) {
            return std::make_unique<TableRef>(std::move(join));
        }

        /** @param v Visitor pattern for the table reference */
        void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
            v->Visit(common::ManagedPointer(this));
        }

        /** @return The table reference type */
        TableReferenceType GetTableReferenceType() {
            return type_;
        }

        /** @return The table alias */
        AliasType &GetAlias() {
            if (alias_.Empty()) {
                alias_ = AliasType(GetTableName());
            }
            return alias_;
        }

        /** @return The column alias names */
        std::vector<AliasType> GetCteColumnAliases() {
            return cte_col_aliases_;
        }

        /** @return The syntactic type of the CTE (CTEType;:INVALID if TableRef does not correspond to CTE) */
        CteType GetCteType() const {
            return cte_type_;
        }

        /** @return `true` if this table reference represents a CTE temporary table, `false` otherwise */
        bool IsCte() const {
            return cte_type_ != CteType::INVALID;
        }

        /** @return `true` if this table reference represents an inductive CTE, `false` otherwise */
        bool IsSyntacticallyInductiveCte() const {
            return (cte_type_ == CteType::RECURSIVE) || (cte_type_ == CteType::ITERATIVE)
                   || (cte_type_ == CteType::STRUCTURALLY_RECURSIVE) || (cte_type_ == CteType::STRUCTURALLY_ITERATIVE);
        }

        /** @return `true` if this table reference represents a CTE with inductive structure, `false` otherwise */
        bool IsStructurallyInductiveCte() const {
            return (cte_type_ == CteType::STRUCTURALLY_RECURSIVE) || (cte_type_ == CteType::STRUCTURALLY_ITERATIVE);
        }

        /** @return The table name */
        const std::string &GetTableName() {
            return table_info_->GetTableName();
        }

        /** @return The namespace name */
        const std::string &GetNamespaceName() {
            return table_info_->GetNamespaceName();
        }

        /** @return The database name */
        const std::string &GetDatabaseName() {
            return table_info_->GetDatabaseName();
        }

        /** @return The SELECT statement */
        common::ManagedPointer<SelectStatement> GetSelect() {
            return common::ManagedPointer(select_);
        }

        /** @return `true` if this table reference has an associated SELECT, `false` otherwise */
        bool HasSelect() const {
            return static_cast<bool>(select_);
        }

        /** @return list of table references */
        std::vector<common::ManagedPointer<TableRef>> GetList() {
            std::vector<common::ManagedPointer<TableRef>> list;
            list.reserve(list_.size());
            for (const auto &item : list_) {
                list.emplace_back(common::ManagedPointer(item));
            }
            return list;
        }

        /** @return The join */
        common::ManagedPointer<JoinDefinition> GetJoin() {
            return common::ManagedPointer(join_);
        }

        /** @return The hashed value of this table ref object */
        common::hash_t Hash() const;

        /**
         * Logical equality check.
         * @param rhs other
         * @return true if the two TableRef are logically equal
         */
        bool operator==(const TableRef &rhs) const;

        /**
         * Logical inequality check.
         * @param rhs other
         * @return true if the two TabelRef are logically unequal
         */
        bool operator!=(const TableRef &rhs) const {
            return !(operator==(rhs));
        }

        /**
         * Inserts all the table aliases forming a table ref into the input set.
         * (i.e., all the aliases used in the FROM clause, including all aliases in all JOINs)
         * @param aliases set to insert aliases into
         */
        void GetConstituentTableAliases(std::vector<std::string> *aliases);

        /** @return TableRef serialized to json */
        nlohmann::json ToJson() const;

        /** @param j json to deserialize */
        std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

    private:
        friend class binder::BindNodeVisitor;

        // TODO(Kyle): It seems that this implementation of TableRef
        // is trying to be "too general" - maybe we should look into
        // breaking up the implementation between e.g. table references
        // that are defined SELECT statements vs those that are defined
        // by JOINs, etc. as it might simplify the logic considerably

        // The type of the table reference
        TableReferenceType type_;

        // The alias for the table reference
        AliasType alias_;

        // Associated information for the referenced table (if applicable)
        std::unique_ptr<TableInfo> table_info_;

        // The SELECT statement associated with the table reference (if applicable)
        std::unique_ptr<SelectStatement> select_;

        // The column aliases provided by this CTE
        std::vector<AliasType> cte_col_aliases_;

        // The CTE type represented by this table reference
        CteType cte_type_{CteType::INVALID};

        // List of constitutent table references (if applicable)
        std::vector<std::unique_ptr<TableRef>> list_;

        // The JOIN definition (if applicable)
        std::unique_ptr<JoinDefinition> join_;
    };

    DEFINE_JSON_HEADER_DECLARATIONS(TableRef);

} // namespace parser
} // namespace noisepage
