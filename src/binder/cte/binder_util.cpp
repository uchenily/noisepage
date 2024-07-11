#include "binder/cte/binder_util.h"

#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"

namespace noisepage::binder::cte {

auto BinderUtil::GetSelectWithOrder(common::ManagedPointer<parser::SelectStatement> select_statement)
    -> std::vector<common::ManagedPointer<parser::TableRef>> {
    throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... SELECT Not Implemeneted");
}

auto BinderUtil::GetInsertWithOrder(common::ManagedPointer<parser::InsertStatement> insert_statement)
    -> std::vector<common::ManagedPointer<parser::TableRef>> {
    throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... INSERT Not Implemeneted");
}

auto BinderUtil::GetUpdateWithOrder(common::ManagedPointer<parser::UpdateStatement> update_statement)
    -> std::vector<common::ManagedPointer<parser::TableRef>> {
    throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... UPDATE Not Implemeneted");
}

auto BinderUtil::GetDeleteWithOrder(common::ManagedPointer<parser::DeleteStatement> delete_statement)
    -> std::vector<common::ManagedPointer<parser::TableRef>> {
    throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... DELETE Not Implemeneted");
}
} // namespace noisepage::binder::cte
