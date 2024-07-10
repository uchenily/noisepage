#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_constraint_impl.h"
#include "catalog/postgres/pg_core_impl.h"
#include "catalog/postgres/pg_language_impl.h"
#include "catalog/postgres/pg_proc_impl.h"
#include "catalog/postgres/pg_statistic_impl.h"
#include "catalog/postgres/pg_type_impl.h"
#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/table_stats.h"

namespace noisepage::transaction {
class TransactionContext;
} // namespace noisepage::transaction

namespace noisepage::storage {
class GarbageCollector;
class RecoveryManager;
class SqlTable;
namespace index {
    class Index;
} // namespace index
} // namespace noisepage::storage

namespace noisepage::catalog {
/**
 * DatabaseCatalog 存储了有关用户表(user tables)和用户定义的数据库对象(user-defined database objects)的所有元数据
 * 所以系统的其他部分(i.e., binder, optimizer, and execution engine)
 * 可以对这些对象进行推理并执行操作.
 *
 * @warning     只有Catalog、CatalogAccessor和RecoveryManager应该使用下面的接口
 *              所有其他代码都应该使用CatalogAccessor API, 因为它:
 *              - 限定作用域到特定数据库
 *              - 处理在该数据库中查找表的命名空间解析(namespace resolution)
 */
class DatabaseCatalog {
public:
    /**
     * @brief 用默认entry启动整个catalog
     * @param txn         The transaction to bootstrap in.
     */
    void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn);

    /** @brief Create a new namespace, may fail with INVALID_NAMESPACE_OID. @see PgCoreImpl::CreateNamespace */
    auto CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name)
        -> namespace_oid_t;
    /** @brief Delete the specified namespace. @see PgCoreImpl::DeleteNamespace */
    auto DeleteNamespace(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid) -> bool;
    /** @brief 获取指定命名空间的oid @see PgCoreImpl::GetNamespaceOid */
    auto GetNamespaceOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name)
        -> namespace_oid_t;

    /** @brief Create a new table, may fail with INVALID_TABLE_OID. @see PgCoreImpl::CreateTable */
    auto CreateTable(common::ManagedPointer<transaction::TransactionContext> txn,
                     namespace_oid_t                                         ns,
                     const std::string                                      &name,
                     const Schema                                           &schema) -> table_oid_t;
    /** @brief Delete the specified table. @see PgCoreImpl::DeleteTable */
    auto DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) -> bool;
    /** @brief Rename a table. @see PgCoreImpl::RenameTable */
    auto RenameTable(common::ManagedPointer<transaction::TransactionContext> txn,
                     table_oid_t                                             table,
                     const std::string                                      &name) -> bool;

    /**
     * @brief 为指定表设置底层存储的位置(the location of the underlying storage)
     *
     * @param txn         The transaction for the operation.
     * @param table       The OID of the table in the catalog.
     * @param table_ptr   The pointer to the underlying storage in memory.
     * @return            True if the operation was successful. False otherwise.
     *
     * @warning   传入的SqlTable指针必须位于堆上, 因为catalog将获得它的所有权(ownership),
     *            并在适当的时候调度(schedule)GC删除它.
     * @warning   调用此函数后, 在SqlTable指针上调用delete是不安全的. 这与返回状态无关.
     */
    auto SetTablePointer(common::ManagedPointer<transaction::TransactionContext> txn,
                         table_oid_t                                             table,
                         const storage::SqlTable                                *table_ptr) -> bool;
    /**
     * @brief 为指定索引设置底层实现的位置(the location of the underlying implementation)
     *
     * @param txn         The transaction for the operation.
     * @param index       The OID of the index in the catalog.
     * @param index_ptr   The pointer to the underlying index implementation in memory.
     * @return            True if the operation was successful. False otherwise.
     *
     * @warning   传入的Index指针必须在堆上, 因为catalog将获得它的所有权(ownership),
     * 并在适当的时候通过GC调度(schedule)删除它.
     * @warning   在调用此函数后对索引指针调用delete是不安全的. 这与返回状态无关.
     */
    auto SetIndexPointer(common::ManagedPointer<transaction::TransactionContext> txn,
                         index_oid_t                                             index,
                         storage::index::Index                                  *index_ptr) -> bool;

    /** @brief 获取指定表的OID, 如果不存在这样的REGULAR_TABLE, 则返回INVALID_TABLE_OID */
    auto GetTableOid(common::ManagedPointer<transaction::TransactionContext> txn,
                     namespace_oid_t                                         ns,
                     const std::string                                      &name) -> table_oid_t;
    /** @brief 获取指定索引的OID, 如果不存在这样的索引, 则返回INVALID_INDEX_OID. (x: OID应该是object id的意思) */
    auto GetIndexOid(common::ManagedPointer<transaction::TransactionContext> txn,
                     namespace_oid_t                                         ns,
                     const std::string                                      &name) -> index_oid_t;

    /** @brief Get the storage pointer for the specified table, or nullptr if no such REGULAR_TABLE exists. */
    auto GetTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table)
        -> common::ManagedPointer<storage::SqlTable>;
    /** @brief Get the index pointer for the specified index, or nullptr if no such INDEX exists. */
    auto GetIndex(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index)
        -> common::ManagedPointer<storage::index::Index>;

    /** @brief Get the name of the specified index */
    auto GetIndexName(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index)
        -> std::string_view;

    /** @brief Get the schema for the specified table. */
    auto GetSchema(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) -> const Schema &;
    /** @brief Get the index schema for the specified index. */
    auto GetIndexSchema(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index)
        -> const IndexSchema &;

    /**
     * @brief 更新表的模式(schema)
     *
     * 对给定表应用新的schema
     * 这些更改将修改catalog提供的最新schema
     * 不保证修改后列(column)的oid在模式更改期间是稳定的
     *
     * @param txn         The transaction to update the table's schema in.
     * @param table       The table whose schema should be updated.
     * @param new_schema  The new schema to update the table to.
     * @return            True if the update succeeded. False otherwise.
     *
     * @warning           catalog assessor 假定它拥有传入的schema对象的所有权(ownership)
     *                    因此, 不保证该函数返回时指针仍然有效.
     *                    如果调用者在调用之后需要引用schema对象,
     *                    那么调用者应该使用GetSchema函数来获取该表的可靠(authoritative) schema.
     */
    auto UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn,
                      table_oid_t                                             table,
                      Schema                                                 *new_schema) -> bool;

    /** @brief Create an index, may fail with INVALID_INDEX_OID. @see PgCoreImpl::CreateIndex */
    auto CreateIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                     namespace_oid_t                                         ns,
                     const std::string                                      &name,
                     table_oid_t                                             table,
                     const IndexSchema                                      &schema) -> index_oid_t;
    /** @brief Delete the specified index. @see PgCoreImpl::DeleteIndex */
    auto DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index) -> bool;
    /** @brief Get all of the index OIDs for a specific table. @see PgCoreImpl::GetIndexOids */
    auto GetIndexOids(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table)
        -> std::vector<index_oid_t>;
    /** @brief More efficient way of getting all the indexes for a specific table. @see PgCoreImpl::GetIndexes */
    auto GetIndexes(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table)
        -> std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>>;

    /** @return The type_oid_t that corresponds to the internal TypeId. */
    auto GetTypeOidForType(execution::sql::SqlTypeId type) -> type_oid_t;

    /** @brief 获取表的所有约束(constraint)列表 */
    auto GetConstraints(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table)
        -> std::vector<constraint_oid_t>;

    /** @brief Create a new language, may fail with INVALID_LANGUAGE_OID. @see PgLanguageImpl::CreateLanguage */
    auto CreateLanguage(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &lanname)
        -> language_oid_t;
    /** @brief Drop the specified language. @see PgLanguageImpl::DropLanguage */
    auto DropLanguage(common::ManagedPointer<transaction::TransactionContext> txn, language_oid_t oid) -> bool;
    /** @brief Get the OID of the specified language. @see PgLanguageImpl::GetLanguageOid */
    auto GetLanguageOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &lanname)
        -> language_oid_t;

    /** @brief Create a new procedure, may fail with INVALID_PROC_OID. @see PgProcImpl::CreateProcedure */
    auto CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn,
                         const std::string                                      &procname,
                         language_oid_t                                          language_oid,
                         namespace_oid_t                                         procns,
                         type_oid_t                                              variadic_type,
                         const std::vector<std::string>                         &args,
                         const std::vector<type_oid_t>                          &arg_types,
                         const std::vector<type_oid_t>                          &all_arg_types,
                         const std::vector<postgres::PgProc::ArgModes>          &arg_modes,
                         type_oid_t                                              rettype,
                         const std::string                                      &src,
                         bool                                                    is_aggregate) -> proc_oid_t;
    /** @brief Drop the specified procedure. @see PgProcImpl::DropProcedure */
    auto DropProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc) -> bool;
    /** @brief Get the OID of the specified procedure. @see PgProcImpl::GetProcOid */
    auto GetProcOid(common::ManagedPointer<transaction::TransactionContext> txn,
                    namespace_oid_t                                         procns,
                    const std::string                                      &procname,
                    const std::vector<type_oid_t>                          &all_arg_types) -> proc_oid_t;
    /** @brief Set the procedure context for the specified procedure. @see PgProcImpl::SetFunctionContextPointer */
    auto SetFunctionContextPointer(common::ManagedPointer<transaction::TransactionContext> txn,
                                   proc_oid_t                                              proc_oid,
                                   const execution::functions::FunctionContext            *func_context) -> bool;
    /** @brief Get the procedure context for the specified procedure. @see PgProcImpl::GetFunctionContext */
    auto GetFunctionContext(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid)
        -> common::ManagedPointer<execution::functions::FunctionContext>;

    /** @brief 获取指定列的统计信息(statistics). @see PgStatisticImpl::GetColumnStatistics */
    auto GetColumnStatistics(common::ManagedPointer<transaction::TransactionContext> txn,
                             table_oid_t                                             table_oid,
                             col_oid_t col_oid) -> std::unique_ptr<optimizer::ColumnStatsBase>;

    /** @brief 获取指定表的统计信息(statistics). @see PgStatisticImpl::GetTableStatistics */
    auto GetTableStatistics(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid)
        -> optimizer::TableStats;

private:
    /**
     * The maximum number of tuples to be read out at a time when scanning tables during teardown.
     * This is arbitrary and defined here so that all PgBlahImpl classes can use the same value.
     */
    static constexpr uint32_t TEARDOWN_MAX_TUPLES = 100;

    /**
     * DatabaseCatalog methods generally handle coarse-grained(粗粒度) locking. The various PgXXXImpl classes need to
     * invoke private DatabaseCatalog methods such as CreateTableEntry and CreateIndexEntry during the Bootstrap
     * process.
     */
    ///@{
    friend class postgres::PgCoreImpl;
    friend class postgres::PgConstraintImpl;
    friend class postgres::PgLanguageImpl;
    friend class postgres::PgProcImpl;
    friend class postgres::PgTypeImpl;
    friend class postgres::PgStatisticImpl;
    ///@}
    friend class Catalog;                  ///< Accesses write_lock_ (creating accessor) and TearDown (cleanup).
    friend class postgres::Builder;        ///< Initializes DatabaseCatalog's tables.
    friend class storage::RecoveryManager; ///< Directly modifies DatabaseCatalog's tables.

    // Miscellaneous state.
    std::atomic<uint32_t> next_oid_;                   ///< The next OID, shared across different pg tables.
    std::atomic<transaction::timestamp_t> write_lock_; ///< Used to prevent concurrent DDL change.(防止并发DDL修改)
    const db_oid_t db_oid_; ///< The OID of the database that this DatabaseCatalog is established in.
    const common::ManagedPointer<storage::GarbageCollector> garbage_collector_; ///< The garbage collector used.

    // The Postgres tables.
    postgres::PgCoreImpl       pg_core_; ///< Core Postgres tables: pg_namespace, pg_class, pg_index, pg_attribute.
    postgres::PgTypeImpl       pg_type_; ///< Types: pg_type.
    postgres::PgConstraintImpl pg_constraint_; ///< Constraints: pg_constraint.
    postgres::PgLanguageImpl   pg_language_;   ///< Languages: pg_language.
    postgres::PgProcImpl       pg_proc_;       ///< Procedures: pg_proc.
    postgres::PgStatisticImpl  pg_stat_;       ///< Statistics: pg_statistic.

    /** @brief 创建一个新的DatabaseCataLog. 在调用Bootstrap之前不会创建任何表 */
    DatabaseCatalog(db_oid_t oid, common::ManagedPointer<storage::GarbageCollector> garbage_collector);

    /**
     * @brief 为catalog创建所有的ProjectedRowInitializer和ProjectionMap对象
     *        The initializers and maps can be stashed because the catalog should not undergo schema changes at runtime.
     */
    void BootstrapPRIs();

    /** @brief 清理DatabaseCatalog维护的表和索引 */
    void TearDown(common::ManagedPointer<transaction::TransactionContext> txn);

    /**
     * @brief Lock the DatabaseCatalog to disallow concurrent DDL changes. (锁定DatabaseCatalog以禁止并发DDL更改)
     *
     * DatabaseCatalog的内部函数，禁止并发DDL更改
     * 也不允许旧的事务在新事务提交(commit)了DDL更改之后执行(enact)更改
     * 这有效地遵循了(effectively folows)与存储层中的版本指针MVCC相同的时间戳排序逻辑(timestamp ordering logic),
     * 它还序列化(serializes)数据库中的所有DDL
     *
     * @param txn     请求事务
     *                用于检查(inspect)时间戳(timestamp)和注册提交/中止事件(register commit/abort
     * events)，以便在获得锁时释放锁。
     * @return        如果获得了锁, 则为True. 否则False
     *
     * @warning       这要求在提交时间(commit time)存储在TransactionContext的FinishTime之后执行提交操作
     */
    auto TryLock(common::ManagedPointer<transaction::TransactionContext> txn) -> bool;

    /**
     * @brief Atomically update the next oid counter to the max of the current count and the provided next oid.
     * @param oid     The next oid to move the oid counter to.
     */
    void UpdateNextOid(uint32_t oid) {
        uint32_t expected = 0;
        uint32_t desired = 0;
        do {
            expected = next_oid_.load();
            desired = std::max(expected, oid);
        } while (!next_oid_.compare_exchange_weak(expected, desired));
    }

    /** @brief 对CreateTableEntry和SetTablePointer的封装 */
    void BootstrapTable(common::ManagedPointer<transaction::TransactionContext> txn,
                        table_oid_t                                             table_oid,
                        namespace_oid_t                                         ns_oid,
                        const std::string                                      &name,
                        const Schema                                           &schema,
                        common::ManagedPointer<storage::SqlTable>               table_ptr);
    /** @brief 对CreateIndexEntry和SetIndexPointer的封装 */
    void BootstrapIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                        namespace_oid_t                                         ns_oid,
                        table_oid_t                                             table_oid,
                        index_oid_t                                             index_oid,
                        const std::string                                      &name,
                        const IndexSchema                                      &schema,
                        common::ManagedPointer<storage::index::Index>           index_ptr);

    /**
     * @brief 创建一个不带DDL锁(WITHOUT TAKING THE DDL LOCK)的新表项(table entry). 由DatabaseCatalog的其他成员使用.
     * @see   PgCoreImpl::CreateTableEntry
     */
    auto CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn,
                          table_oid_t                                             table_oid,
                          namespace_oid_t                                         ns_oid,
                          const std::string                                      &name,
                          const Schema                                           &schema) -> bool;
    /**
     * @brief 创建一个不带DDL锁(WITHOUT TAKING THE DDL LOCK)的新索引项(index entry). 由DatabaseCataLog的其他成员使用.
     * @see   PgCoreImpl::CreateIndexEntry
     */
    auto CreateIndexEntry(common::ManagedPointer<transaction::TransactionContext> txn,
                          namespace_oid_t                                         ns_oid,
                          table_oid_t                                             table_oid,
                          index_oid_t                                             index_oid,
                          const std::string                                      &name,
                          const IndexSchema                                      &schema) -> bool;

    /**
     * @brief Creates table statistics in pg_statistic. Should only be called on valid tables, currently only called by
     * CreateTableEntry after known to succeed.
     */
    void CreateTableStatisticEntry(common::ManagedPointer<transaction::TransactionContext> txn,
                                   table_oid_t                                             table_oid,
                                   const Schema                                           &schema);
    /**
     * @brief 删除给定表的所有索引
     *
     * 这目前被设计为一个内部函数, 不过如果需要的话, 也可以通过CatalogAccessor公开它
     *
     * @param txn     The transaction to perform the deletions in.
     * @param table   The OID of the table to remove all indexes for.
     * @return        True if the deletion succeeded. False otherwise.
     */
    auto DeleteIndexes(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) -> bool;

    /** @brief Get all the columns for a particular pg_attribute entry. @see PgAttributeImpl::GetColumns */
    template <typename Column, typename ClassOid, typename ColOid>
    auto GetColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid)
        -> std::vector<Column>;

    /**
     * @brief 在pg_class中设置表的schema
     *
     * @tparam CallerType     The type of the caller. Should only be used by recovery!
     * @param txn             The transaction to perform the schema change in.
     * @param oid             The OID of the table.
     * @param schema          The new schema to set.
     * @return                True if the schema was set successfully. False otherwise.
     */
    template <typename CallerType>
    auto SetTableSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn,
                               table_oid_t                                             oid,
                               const Schema                                           *schema) -> bool {
        static_assert(std::is_same_v<CallerType, storage::RecoveryManager>, "Only recovery should call this.");
        return SetClassPointer(txn, oid, schema, postgres::PgClass::REL_SCHEMA.oid_);
    }

    /**
     * @brief 在pg_class中设置索引的schema
     *
     * @tparam CallerType     The type of the caller. Should only be used by recovery!
     * @param txn             The transaction to perform the schema change in.
     * @param oid             The OID of the index.
     * @param schema          The new index schema to set.
     * @return                True if the index schema was set successfully. False otherwise.
     */
    template <typename CallerType>
    auto SetIndexSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn,
                               index_oid_t                                             oid,
                               const IndexSchema                                      *schema) -> bool {
        static_assert(std::is_same_v<CallerType, storage::RecoveryManager>, "Only recovery should call this.");
        return SetClassPointer(txn, oid, schema, postgres::PgClass::REL_SCHEMA.oid_);
    }

    /** @brief 为指定的pg_class列设置REL_PTR. @see PgCoreImpl::SetClassPointer */
    template <typename ClassOid, typename Ptr>
    auto SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn,
                         ClassOid                                                oid,
                         const Ptr                                              *pointer,
                         col_oid_t                                               class_col) -> bool;
};
} // namespace noisepage::catalog
