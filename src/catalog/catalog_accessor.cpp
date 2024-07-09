#include "catalog/catalog_accessor.h"

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_cache.h"
#include "catalog/database_catalog.h"
#include "catalog/postgres/pg_proc.h"

namespace noisepage::catalog {
auto CatalogAccessor::GetDatabaseOid(std::string name) const -> db_oid_t {
    NormalizeObjectName(&name);
    return catalog_->GetDatabaseOid(txn_, name);
}

auto CatalogAccessor::CreateDatabase(std::string name) const -> db_oid_t {
    NormalizeObjectName(&name);
    return catalog_->CreateDatabase(txn_, name, true);
}

auto CatalogAccessor::DropDatabase(db_oid_t db) const -> bool {
    return catalog_->DeleteDatabase(txn_, db);
}

void CatalogAccessor::SetSearchPath(std::vector<namespace_oid_t> namespaces) {
    NOISEPAGE_ASSERT(!namespaces.empty(), "search path cannot be empty");

    default_namespace_ = namespaces[0];
    search_path_ = std::move(namespaces);

    // Check if 'pg_catalog is explicitly set'
    for (const auto &ns : search_path_) {
        if (ns == postgres::PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID) {
            return;
        }
    }

    search_path_.emplace(search_path_.begin(), postgres::PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID);
}

auto CatalogAccessor::GetNamespaceOid(std::string name) const -> namespace_oid_t {
    if (name.empty()) {
        return catalog::postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID;
    }
    NormalizeObjectName(&name);
    return dbc_->GetNamespaceOid(txn_, name);
}

auto CatalogAccessor::CreateNamespace(std::string name) const -> namespace_oid_t {
    NormalizeObjectName(&name);
    return dbc_->CreateNamespace(txn_, name);
}

auto CatalogAccessor::DropNamespace(namespace_oid_t ns) const -> bool {
    return dbc_->DeleteNamespace(txn_, ns);
}

auto CatalogAccessor::GetTableOid(std::string name) const -> table_oid_t {
    NormalizeObjectName(&name);
    for (const auto &path : search_path_) {
        table_oid_t search_result = dbc_->GetTableOid(txn_, path, name);
        if (search_result != INVALID_TABLE_OID) {
            return search_result;
        }
    }
    return INVALID_TABLE_OID;
}

auto CatalogAccessor::GetTableOid(namespace_oid_t ns, std::string name) const -> table_oid_t {
    NormalizeObjectName(&name);
    return dbc_->GetTableOid(txn_, ns, name);
}

auto CatalogAccessor::CreateTable(namespace_oid_t ns, std::string name, const Schema &schema) const -> table_oid_t {
    NormalizeObjectName(&name);
    return dbc_->CreateTable(txn_, ns, name, schema);
}

auto CatalogAccessor::RenameTable(table_oid_t table, std::string new_table_name) const -> bool {
    NormalizeObjectName(&new_table_name);
    return dbc_->RenameTable(txn_, table, new_table_name);
}

auto CatalogAccessor::DropTable(table_oid_t table) const -> bool {
    return dbc_->DeleteTable(txn_, table);
}

auto CatalogAccessor::SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) const -> bool {
    return dbc_->SetTablePointer(txn_, table, table_ptr);
}

auto CatalogAccessor::GetTable(const table_oid_t table) const -> common::ManagedPointer<storage::SqlTable> {
    if (UNLIKELY(catalog::IsTempOid(table))) {
        auto result = temp_tables_.find(table);
        NOISEPAGE_ASSERT(result != temp_tables_.end(), "temp_tables_ does not contain desired table");
        return result->second;
    }
    if (cache_ != DISABLED) {
        auto table_ptr = cache_->GetTable(table);
        if (table_ptr == nullptr) {
            // not in the cache, get it from the actual catalog, stash it, and return retrieved value
            table_ptr = dbc_->GetTable(txn_, table);
            cache_->PutTable(table, table_ptr);
        }
        return table_ptr;
    }
    return dbc_->GetTable(txn_, table);
}

auto CatalogAccessor::UpdateSchema(table_oid_t table, Schema *new_schema) const -> bool {
    return dbc_->UpdateSchema(txn_, table, new_schema);
}

auto CatalogAccessor::GetSchema(const table_oid_t table) const -> const Schema & {
    if (UNLIKELY(catalog::IsTempOid(table))) {
        auto result = temp_schemas_.find(table);
        NOISEPAGE_ASSERT(result != temp_schemas_.end(), "temp_tables_ does not contain desired table");
        return *(result->second);
    }
    return dbc_->GetSchema(txn_, table);
}

auto CatalogAccessor::GetConstraints(table_oid_t table) const -> std::vector<constraint_oid_t> {
    return dbc_->GetConstraints(txn_, table);
}

auto CatalogAccessor::GetIndexOids(table_oid_t table) const -> std::vector<index_oid_t> {
    if (cache_ != DISABLED) {
        auto cache_lookup = cache_->GetIndexOids(table);
        if (!cache_lookup.first) {
            // not in the cache, get it from the actual catalog, stash it, and return retrieved value
            const auto index_oids = dbc_->GetIndexOids(txn_, table);
            cache_->PutIndexOids(table, index_oids);
            return index_oids;
        }
        return cache_lookup.second;
    }
    return dbc_->GetIndexOids(txn_, table);
}

auto CatalogAccessor::GetIndexes(const table_oid_t table)
    -> std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> {
    return dbc_->GetIndexes(txn_, table);
}

auto CatalogAccessor::GetIndexOid(std::string name) const -> index_oid_t {
    NormalizeObjectName(&name);
    for (const auto &path : search_path_) {
        const index_oid_t search_result = dbc_->GetIndexOid(txn_, path, name);
        if (search_result != INVALID_INDEX_OID) {
            return search_result;
        }
    }
    return INVALID_INDEX_OID;
}

auto CatalogAccessor::GetIndexOid(namespace_oid_t ns, std::string name) const -> index_oid_t {
    NormalizeObjectName(&name);
    return dbc_->GetIndexOid(txn_, ns, name);
}

auto CatalogAccessor::CreateIndex(namespace_oid_t    ns,
                                  table_oid_t        table,
                                  std::string        name,
                                  const IndexSchema &schema) const -> index_oid_t {
    NormalizeObjectName(&name);
    return dbc_->CreateIndex(txn_, ns, name, table, schema);
}

auto CatalogAccessor::GetIndexSchema(index_oid_t index) const -> const IndexSchema & {
    return dbc_->GetIndexSchema(txn_, index);
}

auto CatalogAccessor::DropIndex(index_oid_t index) const -> bool {
    return dbc_->DeleteIndex(txn_, index);
}

auto CatalogAccessor::SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr) const -> bool {
    return dbc_->SetIndexPointer(txn_, index, index_ptr);
}

auto CatalogAccessor::GetIndex(index_oid_t index) const -> common::ManagedPointer<storage::index::Index> {
    if (cache_ != DISABLED) {
        auto index_ptr = cache_->GetIndex(index);
        if (index_ptr == nullptr) {
            // not in the cache, get it from the actual catalog, stash it, and return retrieved value
            index_ptr = dbc_->GetIndex(txn_, index);
            cache_->PutIndex(index, index_ptr);
        }
        return index_ptr;
    }
    return dbc_->GetIndex(txn_, index);
}

auto CatalogAccessor::GetIndexName(index_oid_t index) const -> std::string_view {
    return dbc_->GetIndexName(txn_, index);
}

auto CatalogAccessor::CreateLanguage(const std::string &lanname) -> language_oid_t {
    return dbc_->CreateLanguage(txn_, lanname);
}

auto CatalogAccessor::GetLanguageOid(const std::string &lanname) -> language_oid_t {
    return dbc_->GetLanguageOid(txn_, lanname);
}

auto CatalogAccessor::DropLanguage(language_oid_t language_oid) -> bool {
    return dbc_->DropLanguage(txn_, language_oid);
}

auto CatalogAccessor::CreateProcedure(const std::string                             &procname,
                                      const language_oid_t                           language_oid,
                                      const namespace_oid_t                          procns,
                                      const type_oid_t                               variadic_type,
                                      const std::vector<std::string>                &args,
                                      const std::vector<type_oid_t>                 &arg_types,
                                      const std::vector<type_oid_t>                 &all_arg_types,
                                      const std::vector<postgres::PgProc::ArgModes> &arg_modes,
                                      const type_oid_t                               rettype,
                                      const std::string                             &src,
                                      const bool                                     is_aggregate) -> proc_oid_t {
    return dbc_->CreateProcedure(txn_,
                                 procname,
                                 language_oid,
                                 procns,
                                 variadic_type,
                                 args,
                                 arg_types,
                                 all_arg_types,
                                 arg_modes,
                                 rettype,
                                 src,
                                 is_aggregate);
}

auto CatalogAccessor::DropProcedure(proc_oid_t proc_oid) -> bool {
    return dbc_->DropProcedure(txn_, proc_oid);
}

auto CatalogAccessor::GetProcOid(const std::string &procname, const std::vector<type_oid_t> &arg_types) -> proc_oid_t {
    proc_oid_t ret;
    for (auto ns_oid : search_path_) {
        ret = dbc_->GetProcOid(txn_, ns_oid, procname, arg_types);
        if (ret != catalog::INVALID_PROC_OID) {
            return ret;
        }
    }
    return catalog::INVALID_PROC_OID;
}

auto CatalogAccessor::SetFunctionContextPointer(proc_oid_t                                   proc_oid,
                                                const execution::functions::FunctionContext *func_context) -> bool {
    return dbc_->SetFunctionContextPointer(txn_, proc_oid, func_context);
}

auto CatalogAccessor::GetFunctionContext(proc_oid_t proc_oid)
    -> common::ManagedPointer<execution::functions::FunctionContext> {
    return dbc_->GetFunctionContext(txn_, proc_oid);
}

auto CatalogAccessor::GetColumnStatistics(table_oid_t table_oid, col_oid_t col_oid)
    -> std::unique_ptr<optimizer::ColumnStatsBase> {
    return dbc_->GetColumnStatistics(txn_, table_oid, col_oid);
}

auto CatalogAccessor::GetTableStatistics(table_oid_t table_oid) -> optimizer::TableStats {
    return dbc_->GetTableStatistics(txn_, table_oid);
}

auto CatalogAccessor::GetTypeOidFromTypeId(execution::sql::SqlTypeId type) -> type_oid_t {
    return dbc_->GetTypeOidForType(type);
}

auto CatalogAccessor::GetBlockStore() const -> common::ManagedPointer<storage::BlockStore> {
    // TODO(Matt): at some point we may decide to adjust the source  (i.e. each DatabaseCatalog has one), stick it in a
    // pg_tablespace table, or we may eliminate the concept entirely. This works for now to allow CREATE nodes to bind a
    // BlockStore
    return catalog_->GetBlockStore();
}

void CatalogAccessor::RegisterTempTable(table_oid_t                                         table_oid,
                                        const common::ManagedPointer<storage::SqlTable>     table,
                                        const common::ManagedPointer<const catalog::Schema> schema) {
    temp_tables_[table_oid] = table;
    temp_schemas_[table_oid] = schema;
}

} // namespace noisepage::catalog
