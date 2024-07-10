#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/connection_handle.h"
#include "network/postgres/portal.h"
#include "network/postgres/postgres_command_factory.h"
#include "network/postgres/postgres_network_commands.h"
#include "network/postgres/postgres_packet_writer.h"
#include "network/postgres/statement.h"
#include "network/postgres/statement_cache.h"
#include "network/protocol_interpreter.h"

namespace noisepage::network {

constexpr uint32_t INITIAL_BACKOFF_TIME = 2;
constexpr uint32_t BACKOFF_FACTOR = 2;
constexpr uint32_t MAX_BACKOFF_TIME = 20;

/**
 * Interprets the network protocol for postgres clients. Any state/logic that is Postgres protocol-specific should live
 * at this layer.
 */
class PostgresProtocolInterpreter : public ProtocolInterpreter {
public:
    /**
     * The provider encapsulates the creation logic of a protocol interpreter into an object
     */
    struct Provider : public ProtocolInterpreterProvider {
    public:
        /**
         * Constructs a new provider
         * @param command_factory The command factory to use for the constructed protocol interpreters
         */
        explicit Provider(common::ManagedPointer<PostgresCommandFactory> command_factory)
            : command_factory_(command_factory) {}

        /**
         * @return an instance of the protocol interpreter
         */
        auto Get() -> std::unique_ptr<ProtocolInterpreter> override {
            return std::make_unique<PostgresProtocolInterpreter>(command_factory_);
        }

    private:
        common::ManagedPointer<PostgresCommandFactory> command_factory_;
    };

    /**
     * Creates the interpreter for Postgres
     * @param command_factory to convert packet into commands
     */
    explicit PostgresProtocolInterpreter(common::ManagedPointer<PostgresCommandFactory> command_factory)
        : command_factory_(command_factory) {}

    /**
     * @see ProtocolIntepreter::Process
     * @param in buffer to read packets from
     * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
     * @param taskflow non-owning pointer to the taskflow to pass down to the command layer
     * @param context connection-specific (not protocol) state
     * @return next transition for ConnectionHandle's state machine
     */
    auto Process(common::ManagedPointer<ReadBuffer>         in,
                 common::ManagedPointer<WriteQueue>         out,
                 common::ManagedPointer<taskflow::Taskflow> taskflow,
                 common::ManagedPointer<ConnectionContext>  context) -> Transition override;

    /**
     * Closes any protocol-specific state. We currently use this to remove the temporary namespace for Postgres.
     * @param in buffer to read packets from
     * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
     * @param taskflow non-owning pointer to the taskflow to pass down to the command layer
     * @param context connection-specific (not protocol) state
     */
    void Teardown(common::ManagedPointer<ReadBuffer>         in,
                  common::ManagedPointer<WriteQueue>         out,
                  common::ManagedPointer<taskflow::Taskflow> taskflow,
                  common::ManagedPointer<ConnectionContext>  context) override;

    // TODO(Tianyu): Fill in the following documentation at some point
    /**
     * (Matt): I think this was used in Peloton for asynchronous execution, after a callback wakeup the ConnectionHandle
     * would call to the protocol interpreter to get the state of the asynchronous query after the output was already
     * complete.
     * @param out
     */
    void GetResult(const common::ManagedPointer<WriteQueue> out) override {}

    /**
     * Used to clear the waiting for sync, explicit txn block, and portals. Call whenever a transaction is ended.
     */
    void ResetTransactionState() {
        waiting_for_sync_ = false;
        explicit_txn_block_ = false;
        portals_.clear();
    }

    /**
     * Handles all of the set up for the Postgres protocol. Currently that includes saying no to SSL support during
     * handshake, checking protocol version, parsing client arguments, and creating the temporary namespace for this
     * connection.
     * @param in buffer to read packets from
     * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
     * @param taskflow non-owning pointer to the taskflow for use in any cleanup necessary
     * @param context where to stash connection-specific state
     * @return transition::PROCEED if it succeeded, transition::TERMINATE otherwise
     */
    auto ProcessStartup(common::ManagedPointer<ReadBuffer>         in,
                        common::ManagedPointer<WriteQueue>         out,
                        common::ManagedPointer<taskflow::Taskflow> taskflow,
                        common::ManagedPointer<ConnectionContext>  context) -> Transition;

    /**
     * @return true if the current transaction was initiated with a BEGIN statement
     */
    auto ExplicitTransactionBlock() const -> bool {
        return explicit_txn_block_;
    }

    /**
     * Sets the flag that the current transaction was initiated with a BEGIN statement, false otherwise
     */
    void SetExplicitTransactionBlock() {
        NOISEPAGE_ASSERT(!explicit_txn_block_, "Explicit transaction block flag is already set. That seems wrong.");
        explicit_txn_block_ = true;
    }

    /**
     * @return true if the system should ignore all messages until a Sync is received, caused by an error state in the
     * Extended Query protocol
     */
    auto WaitingForSync() const -> bool {
        return waiting_for_sync_;
    }

    /**
     * Sets the flag that we are now waiting for a Sync command, caused by an error state in the Extended Query protocol
     */
    void SetWaitingForSync() {
        NOISEPAGE_ASSERT(!waiting_for_sync_, "Waiting for sync flag is already set. That seems wrong.");
        waiting_for_sync_ = true;
    }

    /**
     * Issued after a Sync command is received from the client when already in a Sync state.
     */
    void ResetWaitingForSync() {
        NOISEPAGE_ASSERT(waiting_for_sync_, "Waiting for sync flag not already set. That seems wrong.");
        waiting_for_sync_ = false;
    }

    /**
     * @param name statement to look up
     * @return managed pointer to statement if it exists, nullptr otherwise
     */
    auto GetStatement(const std::string &name) const -> common::ManagedPointer<network::Statement> {
        const auto it = statements_.find(name);
        if (it != statements_.end()) {
            return {it->second};
        }
        return nullptr;
    }

    /**
     * @param statement statement to take ownership of
     */
    void AddStatementToCache(std::unique_ptr<network::Statement> &&statement) {
        cache_.Add(std::move(statement));
    }

    /**
     * @param query_text key to look up
     * @return Statement if it exists in the cache, otherwise nullptr
     */
    auto LookupStatementInCache(const std::string &query_text) const -> common::ManagedPointer<network::Statement> {
        return cache_.Lookup(query_text);
    }

    /**
     * @param name key
     * @param statement statement to create a mapping to for this name
     */
    void SetStatement(const std::string &name, const common::ManagedPointer<network::Statement> statement) {
        statements_[name] = statement;
    }

    /**
     * 关闭语句(statement). 我们不关心返回值, 因为对不存在的statement调用close并不是错误,
     * 还关闭从该statement构造的任何portal(如果有的话)
     * @param name statement to be removed
     */
    void CloseStatement(const std::string &name) {
        const auto it = statements_.find(name);
        if (it != statements_.end()) {
            ClosePortalsConstructedFromStatement(common::ManagedPointer(it->second));
            statements_.erase(it);
        }
    }

    /**
     * @param name portal to look up
     * @return managed pointer to portal if it exists, nullptr otherwise
     */
    auto GetPortal(const std::string &name) const -> common::ManagedPointer<network::Portal> {
        const auto it = portals_.find(name);
        if (it != portals_.end()) {
            return common::ManagedPointer(it->second);
        }
        return nullptr;
    }

    /**
     * @param name key
     * @param portal portal to take ownership of
     */
    void SetPortal(const std::string &name, std::unique_ptr<network::Portal> &&portal) {
        portals_[name] = std::move(portal);
    }

    /**
     * close a Portal. We don't care about return value since it's not an error to call Close on non-existent portal
     * @param name portal to be removed
     */
    void ClosePortal(const std::string &name) {
        portals_.erase(name);
    }

protected:
    /**
     * @see ProtocolInterpreter::GetPacketHeaderSize
     * Header format: 1 byte message type (only if non-startup)
     *              + 4 byte message size (inclusive of these 4 bytes)
     */
    auto GetPacketHeaderSize() -> size_t override;

    /**
     * @see ProtocolInterpreter::SetPacketMessageType
     */
    void SetPacketMessageType(common::ManagedPointer<ReadBuffer> in) override;

private:
    bool startup_ = true;
    bool waiting_for_sync_ = false;
    bool explicit_txn_block_ = false;

    common::ManagedPointer<PostgresCommandFactory> command_factory_;

    StatementCache cache_;

    // name to statement
    std::unordered_map<std::string, common::ManagedPointer<network::Statement>> statements_;

    // name to portal
    std::unordered_map<std::string, std::unique_ptr<network::Portal>> portals_;

    /**
     * close all Portals constructed from a Statement. We don't care about return value since it's not an error to call
     * Close on non-existent statement
     * @param statement
     */
    void ClosePortalsConstructedFromStatement(const common::ManagedPointer<Statement> statement) {
        for (auto it = portals_.begin(); it != portals_.end();) {
            if (it->second->GetStatement() == statement) {
                it = portals_.erase(it);
            } else {
                it++;
            }
        }
    }
};

} // namespace noisepage::network
