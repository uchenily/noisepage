#pragma once

#include <algorithm>
#include <memory>
#include <string_view>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "transaction/transaction_defs.h"

namespace noisepage::transaction {
class TransactionContext;
class TransactionManager;
} // namespace noisepage::transaction

namespace noisepage::storage {
class GarbageCollector;
class RecoveryManager;
} // namespace noisepage::storage

namespace noisepage::selfdriving::pilot {
class PilotUtil;
} // namespace noisepage::selfdriving::pilot

namespace noisepage::catalog {

class CatalogCache;
class DatabaseCatalog;
class CatalogAccessor;

/**
 * The catalog stores all of the metadata about user tables and user defined
 * database objects so that other parts of the system (i.e. binder, optimizer,
 * and execution engine) can reason about and execute operations on these
 * objects.
 *
 * @warning Only DBMain and CatalogAccessor (and possibly the recovery system)
 * should be using the interface below.  All other code should use the
 * CatalogAccessor API which enforces scoping to a specific database and handles
 * namespace resolution for finding tables within that database.
 */
class Catalog {
public:
    /**
     * Initializes the Catalog object which creates the primary table for databases
     * and bootstraps the default database ("noisepage").  This also constructs the
     * debootstrap logic (i.e. table deallocations) that gets deferred using the
     * action framework in the destructor.
     * @param txn_manager for spawning read-only transactions in destructors
     * @param block_store to use to back catalog tables
     * @param garbage_collector injected GC to register and deregister indexes. Temporary if we change the GC mechanism
     * for BwTree, or replace it entirely?
     * @warning The catalog requires garbage collection and will leak catalog
     * tables if it is disabled.
     */
    Catalog(common::ManagedPointer<transaction::TransactionManager> txn_manager,
            common::ManagedPointer<storage::BlockStore>             block_store,
            common::ManagedPointer<storage::GarbageCollector>       garbage_collector);

    /**
     * Handles destruction of the catalog's members by calling the destructor on
     * all visible database catalog objects.
     * @warning The catalog assumes that any logically visible database objects
     * referenced by the catalog during destruction need to be deallocated by the
     * deferred action.  Therefore, there cannot be any live transactions when
     * the debootstrap event executes.
     * @note This function will begin and commit a read-only transaction
     * through the transaction manager and therefore must be called before the
     * transaction manager is destructed.  It will also defer events that will
     * begin and commit transactions.
     */
    void TearDown();

    /**
     * Creates a new database instance.
     * @param txn that creates the database
     * @param name of the new database
     * @param bootstrap indicates whether or not to perform bootstrap routine
     * @return OID of the database or INVALID_DATABASE_OID if the operation failed
     *   (which should only occur if there is already a database with that name)
     */
    auto CreateDatabase(common::ManagedPointer<transaction::TransactionContext> txn,
                        std::string_view                                        name,
                        bool                                                    bootstrap) -> db_oid_t;

    /**
     * Deletes the given database.  This operation will fail if there is any DDL
     * operation currently in-flight because it will cause a write-write conflict.
     * It could also fail if the OID does not correspond to an existing database.
     * @param txn that deletes the database
     * @param database OID to be deleted
     * @return true if the deletion succeeds, false otherwise
     */
    auto DeleteDatabase(common::ManagedPointer<transaction::TransactionContext> txn, db_oid_t database) -> bool;

    /**
     * Renames the given database.
     * @param txn for the operation
     * @param database OID to be renamed
     * @param name which the database will now have
     * @return true if the operation succeeds, false otherwise
     */
    auto RenameDatabase(common::ManagedPointer<transaction::TransactionContext> txn,
                        db_oid_t                                                database,
                        std::string_view                                        name) -> bool;

    /**
     * Resolve a database name to its OID.
     * @param txn for the catalog query
     * @param name of the database to resolve
     * @return OID of the database or INVALID_DATABASE_OID if it does not exist
     */
    auto GetDatabaseOid(common::ManagedPointer<transaction::TransactionContext> txn, std::string_view name) -> db_oid_t;

    /**
     * Gets the database-specific catalog object.
     * @param txn for the catalog query
     * @param database OID of needed catalog
     * @return DatabaseCatalog object which has catalog information for the
     *   specific database
     */
    auto GetDatabaseCatalog(common::ManagedPointer<transaction::TransactionContext> txn, db_oid_t database)
        -> common::ManagedPointer<DatabaseCatalog>;

    /**
     * Creates a new accessor into the catalog which will handle transactionality and sequencing of catalog operations.
     * @param txn for all subsequent catalog queries
     * @param database in which this transaction is scoped
     * @param cache CatalogCache object for this connection, or nullptr if disabled
     * @return a CatalogAccessor object for use with this transaction
     */
    auto GetAccessor(common::ManagedPointer<transaction::TransactionContext> txn,
                     db_oid_t                                                database,
                     common::ManagedPointer<CatalogCache>                    cache) -> std::unique_ptr<CatalogAccessor>;

    /**
     * @return Catalog's BlockStore
     */
    auto GetBlockStore() const -> common::ManagedPointer<storage::BlockStore>;

private:
    DISALLOW_COPY_AND_MOVE(Catalog);
    friend class storage::RecoveryManager;
    friend class selfdriving::pilot::PilotUtil;
    const common::ManagedPointer<transaction::TransactionManager> txn_manager_;
    const common::ManagedPointer<storage::BlockStore>             catalog_block_store_;
    const common::ManagedPointer<storage::GarbageCollector>       garbage_collector_;
    std::atomic<db_oid_t>                                         next_oid_;

    storage::SqlTable               *databases_;
    storage::index::Index           *databases_name_index_;
    storage::index::Index           *databases_oid_index_;
    storage::ProjectedRowInitializer get_database_oid_pri_;
    storage::ProjectedRowInitializer get_database_catalog_pri_;
    storage::ProjectedRowInitializer pg_database_all_cols_pri_;
    storage::ProjectionMap           pg_database_all_cols_prm_;
    storage::ProjectedRowInitializer delete_database_entry_pri_;
    storage::ProjectionMap           delete_database_entry_prm_;

    /**
     * Atomically updates the next oid counter to the max of the current count and the provided next oid
     * @param oid next oid to move oid counter to
     */
    void UpdateNextOid(db_oid_t oid) {
        db_oid_t expected;
        db_oid_t desired;
        do {
            expected = next_oid_.load();
            desired = std::max(expected, oid);
        } while (!next_oid_.compare_exchange_weak(expected, desired));
    }

    /**
     * Helper for CreateDatabase. This method can be used by recovery manager to create a database with a specific oid
     * @param txn that creates the database
     * @param name of the new database
     * @param bootstrap indicates whether or not to perform bootstrap routine
     * @param db_oid oid for new database
     * @return true if creation succeeded, false otherwise
     */
    auto CreateDatabase(common::ManagedPointer<transaction::TransactionContext> txn,
                        std::string_view                                        name,
                        bool                                                    bootstrap,
                        catalog::db_oid_t                                       db_oid) -> bool;

    /**
     * Creates a new database entry.
     * @param txn that creates the database
     * @param db OID of the database
     * @param name of the new database
     * @param dbc database catalog object for the new database
     * @return true if successful, otherwise false
     */
    auto CreateDatabaseEntry(common::ManagedPointer<transaction::TransactionContext> txn,
                             db_oid_t                                                db,
                             std::string_view                                        name,
                             DatabaseCatalog                                        *dbc) -> bool;

    /**
     * Deletes a database entry without scheduling the catalog object for destruction
     * @param txn that delete the database
     * @param db OID of the database
     * @return pointer to the database object if successful, otherwise nullptr
     */
    auto DeleteDatabaseEntry(common::ManagedPointer<transaction::TransactionContext> txn, db_oid_t db)
        -> DatabaseCatalog *;

    /**
     * Creates a lambda that captures the necessary values by value and handles
     * the call to teardown and deleting the master object.
     * @param dbc pointing to the appropriate database catalog object
     * @return the action which can be deferred to the appropriate moment.
     */
    auto DeallocateDatabaseCatalog(DatabaseCatalog *dbc) -> std::function<void()>;
};
} // namespace noisepage::catalog
