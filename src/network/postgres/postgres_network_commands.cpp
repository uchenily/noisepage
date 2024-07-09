#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>
#include <variant>

#include "common/thread_context.h"
#include "metrics/metrics_store.h"
#include "network/network_types.h"
#include "network/network_util.h"
#include "network/postgres/postgres_packet_util.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/postgres/statement.h"
#include "parser/variable_show_statement.h"
#include "taskflow/taskflow.h"

namespace {
/**
 * Write the row description for a SHOW statement.
 *
 * @param out       Packet writer for writing results.
 * @param statement The SHOW statement to be described.
 */
using namespace noisepage;
using namespace noisepage::network;
void WriteShowRowDescription(const common::ManagedPointer<PostgresPacketWriter> writer,
                             const common::ManagedPointer<network::Statement>   statement) {
    // TODO(WAN): This code exists because the SHOW statement does not go through the optimizer and therefore does not
    //  have a corresponding OutputSchema to go through the usual (SELECT) code path in DescribeCommand.
    const auto &show_stmt UNUSED_ATTRIBUTE
        = statement->RootStatement().CastManagedPointerTo<parser::VariableShowStatement>();

    const std::string &param_name = show_stmt->GetName();
    auto               expr = std::make_unique<parser::ConstantValueExpression>(execution::sql::SqlTypeId::Varchar);
    expr->SetAlias(parser::AliasType(param_name));
    std::vector<noisepage::planner::OutputSchema::Column> cols;
    cols.emplace_back(param_name, execution::sql::SqlTypeId::Varchar, std::move(expr));

    writer->WriteRowDescription(cols, {network::FieldFormat::text});
}

auto FinishSimpleQueryCommand(const common::ManagedPointer<PostgresPacketWriter> writer,
                              const common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    writer->WriteReadyForQuery(connection->TransactionState());
    return Transition::PROCEED;
}

void ExecutePortal(const common::ManagedPointer<network::ConnectionContext>    connection_ctx,
                   const common::ManagedPointer<Portal>                        portal,
                   const common::ManagedPointer<network::PostgresPacketWriter> writer,
                   const common::ManagedPointer<taskflow::Taskflow>            taskflow,
                   const bool                                                  explicit_txn_block) {
    taskflow::TaskflowResult result;

    const auto query_type = portal->GetStatement()->GetQueryType();
    const auto physical_plan = portal->OptimizeResult()->GetPlanNode();

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (SqlUtil::DMLQueryType(query_type)) {
        // DML query to put through codegen
        result = taskflow->CodegenPhysicalPlan(connection_ctx, writer, portal);

        // TODO(Matt): do something with result here in case codegen fails

        result = taskflow->RunExecutableQuery(connection_ctx, writer, portal);
    } else if (SqlUtil::CreateQueryType(query_type)) {
        if (explicit_txn_block && query_type == network::QueryType::QUERY_CREATE_DB) {
            writer->WriteError({common::ErrorSeverity::ERROR,
                                "CREATE DATABASE cannot run inside a transaction block",
                                common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
            connection_ctx->Transaction()->SetMustAbort();
            return;
        }
        if (query_type == network::QueryType::QUERY_CREATE_INDEX) {
            result = taskflow->ExecuteCreateStatement(connection_ctx, physical_plan, query_type);
            NOISEPAGE_ASSERT(result.type_ == taskflow::ResultType::COMPLETE,
                             "Got through the binder as a valid index name, so we don't expect this to fail.");
            result = taskflow->CodegenPhysicalPlan(connection_ctx, writer, portal);
            // TODO(Matt): do something with result here in case codegen fails
            result = taskflow->RunExecutableQuery(connection_ctx, writer, portal);
        } else {
            result = taskflow->ExecuteCreateStatement(connection_ctx, physical_plan, query_type);
        }
    } else if (SqlUtil::DropQueryType(query_type)) {
        if (explicit_txn_block && query_type == network::QueryType::QUERY_DROP_DB) {
            writer->WriteError({common::ErrorSeverity::ERROR,
                                "DROP DATABASE cannot run inside a transaction block",
                                common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
            connection_ctx->Transaction()->SetMustAbort();
            return;
        }
        result = taskflow->ExecuteDropStatement(connection_ctx, physical_plan, query_type);
    } else if (query_type == network::QueryType::QUERY_EXPLAIN) {
        result = taskflow->ExecuteExplainStatement(connection_ctx, writer, portal);
    } else if (query_type == network::QueryType::QUERY_SHOW) {
        result = taskflow->ExecuteShowStatement(connection_ctx, writer, portal->GetStatement());
    }

    if (result.type_ == taskflow::ResultType::COMPLETE) {
        NOISEPAGE_ASSERT(std::holds_alternative<uint32_t>(result.extra_), "We're expecting number of rows here.");
        writer->WriteCommandComplete(query_type, std::get<uint32_t>(result.extra_));
    } else {
        NOISEPAGE_ASSERT(result.type_ == taskflow::ResultType::ERROR,
                         "Currently only expecting COMPLETE or ERROR from Taskflow here.");
        NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(result.extra_), "We're expecting a message here.");
        writer->WriteError(std::get<common::ErrorData>(result.extra_));
    }
}
} // namespace

namespace noisepage::network {

auto SimpleQueryCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> writer,
                              const common::ManagedPointer<taskflow::Taskflow>   taskflow,
                              const common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    // Parsing a SimpleQuery clears the unnamed statement and portal
    postgres_interpreter->CloseStatement("");
    postgres_interpreter->ClosePortal("");

    auto query_text = reader_.ReadString();

    auto parse_result = taskflow->ParseQuery(query_text, connection);

    if (std::holds_alternative<common::ErrorData>(parse_result)) {
        writer->WriteError(std::get<common::ErrorData>(parse_result));
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to parse fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        return FinishSimpleQueryCommand(writer, connection);
    }

    const auto statement
        = std::make_unique<network::Statement>(std::move(query_text),
                                               std::move(std::get<std::unique_ptr<parser::ParseResult>>(parse_result)));

    // TODO(Matt): Clients may send multiple statements in a single SimpleQuery packet/string. Handling that would
    // probably exist here, looping over all of the elements in the ParseResult. It's not clear to me how the binder
    // would handle that though since you pass the ParseResult in for binding. Maybe bind ParseResult once?

    // Empty queries get a special response in postgres and do not care if they're in a failed txn block
    if (statement->Empty()) {
        writer->WriteEmptyQueryResponse();
        return FinishSimpleQueryCommand(writer, connection);
    }

    const auto query_type = statement->GetQueryType();

    // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
    if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL
        && query_type != QueryType::QUERY_COMMIT && query_type != QueryType::QUERY_ROLLBACK) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "current transaction is aborted, commands ignored until end of transaction block",
                            common::ErrorCode::ERRCODE_IN_FAILED_SQL_TRANSACTION});
        return FinishSimpleQueryCommand(writer, connection);
    }

    // TODO(WAN): as found by Poojita, some SET TRANSACTION ... and SHOW TRANSACTION ... should be transactional.
    // Set statements are manually handled here. They are not transactional, so handle them before transactional logic.
    if (UNLIKELY(query_type == network::QueryType::QUERY_SET)) {
        if (postgres_interpreter->ExplicitTransactionBlock()) {
            writer->WriteError({common::ErrorSeverity::ERROR,
                                "SET cannot run inside a transaction block",
                                common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
            connection->Transaction()->SetMustAbort();
            return FinishSimpleQueryCommand(writer, connection);
        }

        auto set_result = taskflow->ExecuteSetStatement(connection, common::ManagedPointer(statement));
        if (set_result.type_ == taskflow::ResultType::ERROR) {
            writer->WriteError(std::get<common::ErrorData>(set_result.extra_));
        } else {
            writer->WriteCommandComplete(network::QueryType::QUERY_SET, 0);
        }
        return FinishSimpleQueryCommand(writer, connection);
    }

    // TODO(WAN): this is a temporary hack to unblock Ziqi's oltpbench work. #1188
    if (UNLIKELY(query_type == network::QueryType::QUERY_SHOW)) {
        auto show_result = taskflow->ExecuteShowStatement(connection, writer, common::ManagedPointer(statement));
        NOISEPAGE_ASSERT(show_result.type_ == taskflow::ResultType::COMPLETE,
                         "TODO this should be fixed to handle failure.");
        writer->WriteCommandComplete(network::QueryType::QUERY_SHOW, 0);
        return FinishSimpleQueryCommand(writer, connection);
    }

    // Begin a transaction, regardless of statement type. If it's a BEGIN statement it's implicitly in this txn
    if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
        NOISEPAGE_ASSERT(!postgres_interpreter->ExplicitTransactionBlock(),
                         "We shouldn't be in an explicit txn block is transaction state is IDLE.");
        taskflow->BeginTransaction(connection);
    }

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (SqlUtil::TransactionalQueryType(query_type)) {
        taskflow->ExecuteTransactionStatement(connection,
                                              writer,
                                              postgres_interpreter->ExplicitTransactionBlock(),
                                              query_type);
        if (query_type == network::QueryType::QUERY_BEGIN) {
            if (!(postgres_interpreter->ExplicitTransactionBlock())) {
                postgres_interpreter->SetExplicitTransactionBlock();
            }
        } else {
            postgres_interpreter->ResetTransactionState();
        }
        return FinishSimpleQueryCommand(writer, connection);
    }

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (SqlUtil::UnsupportedQueryType(query_type)) {
        writer->WriteError({common::ErrorSeverity::NOTICE,
                            "we don't yet support that query type.",
                            common::ErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED});
        writer->WriteCommandComplete(query_type, 0);
    } else {
        // NOTE(x): most case
        // Try to bind the parsed statement
        const auto bind_result = taskflow->BindQuery(connection, common::ManagedPointer(statement), nullptr);

        if (bind_result.type_ == taskflow::ResultType::COMPLETE) {
            // Binding succeeded, optimize to generate a physical plan and then execute
            auto optimize_result = taskflow->OptimizeBoundQuery(connection, statement->ParseResult(), nullptr);

            statement->SetOptimizeResult(std::move(optimize_result));

            const auto portal = std::make_unique<Portal>(common::ManagedPointer(statement));

            if (query_type == network::QueryType::QUERY_SELECT) {
                writer->WriteRowDescription(portal->OptimizeResult()->GetPlanNode()->GetOutputSchema()->GetColumns(),
                                            portal->ResultFormats());
            } else if (query_type == network::QueryType::QUERY_EXPLAIN) {
                writer->WriteExplainRowDescription();
            } else if (query_type == network::QueryType::QUERY_SHOW) {
                WriteShowRowDescription(writer, portal->GetStatement());
            }

            ExecutePortal(connection,
                          common::ManagedPointer(portal),
                          writer,
                          taskflow,
                          postgres_interpreter->ExplicitTransactionBlock());
        } else if (bind_result.type_ == taskflow::ResultType::NOTICE) {
            NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(bind_result.extra_),
                             "We're expecting a message here.");
            writer->WriteError(std::get<common::ErrorData>(bind_result.extra_));
            writer->WriteCommandComplete(query_type, 0);
        } else {
            NOISEPAGE_ASSERT(bind_result.type_ == taskflow::ResultType::ERROR,
                             "I don't think we expect any other ResultType at this point.");
            NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(bind_result.extra_),
                             "We're expecting a message here.");
            // failing to bind fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
            writer->WriteError(std::get<common::ErrorData>(bind_result.extra_));
        }
    }

    if (!postgres_interpreter->ExplicitTransactionBlock()) {
        // Single statement transaction should be ended before returning
        // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
        taskflow->EndTransaction(connection,
                                 connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                        : network::QueryType::QUERY_COMMIT);
        postgres_interpreter->ResetTransactionState();
    }

    return FinishSimpleQueryCommand(writer, connection);
}

auto ParseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                        const common::ManagedPointer<PostgresPacketWriter> writer,
                        const common::ManagedPointer<taskflow::Taskflow>   taskflow,
                        const common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    const auto statement_name = reader_.ReadString();

    if (!statement_name.empty() && postgres_interpreter->GetStatement(statement_name) != nullptr) {
        writer->WriteError(
            {common::ErrorSeverity::ERROR,
             "Named prepared statements must be explicitly closed before they can be redefined by another "
             "Parse message.",
             common::ErrorCode::ERRCODE_DUPLICATE_PSTATEMENT});
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to parse fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        return Transition::PROCEED;
    }

    auto query_text = reader_.ReadString();
    auto parse_result = taskflow->ParseQuery(query_text, connection);

    if (std::holds_alternative<common::ErrorData>(parse_result)) {
        writer->WriteError(std::get<common::ErrorData>(parse_result));
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to parse fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        postgres_interpreter->SetWaitingForSync();
        return FinishSimpleQueryCommand(writer, connection);
    }

    auto param_types = PostgresPacketUtil::ReadParamTypes(common::ManagedPointer(&reader_));

    auto statement
        = std::make_unique<network::Statement>(std::move(query_text),
                                               std::move(std::get<std::unique_ptr<parser::ParseResult>>(parse_result)),
                                               std::move(param_types));

    // Extended Query protocol doesn't allow for more than one statement per query string
    if (statement->ParseResult()->NumStatements() > 1) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "extended query only allows one statement per query",
                            common::ErrorCode::ERRCODE_SYNTAX_ERROR});
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to parse fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        postgres_interpreter->SetWaitingForSync();
        return Transition::PROCEED;
    }

    if (SqlUtil::UnsupportedQueryType(statement->GetQueryType())) {
        writer->WriteError({common::ErrorSeverity::NOTICE,
                            "we don't yet support that query type.",
                            common::ErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED});
    }

    auto cached_statement = postgres_interpreter->LookupStatementInCache(statement->GetQueryText());
    if (cached_statement == nullptr) {
        // Not in the cache, add to cache
        cached_statement = common::ManagedPointer(statement);
        postgres_interpreter->AddStatementToCache(std::move(statement));
    }

    postgres_interpreter->SetStatement(statement_name, cached_statement);

    writer->WriteParseComplete();
    return Transition::PROCEED;
}

auto BindCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                       const common::ManagedPointer<PostgresPacketWriter> writer,
                       const common::ManagedPointer<taskflow::Taskflow>   taskflow,
                       const common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    const bool bind_command_metrics_enabled
        = common::thread_context.metrics_store_ != nullptr
          && common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::BIND_COMMAND);
    if (bind_command_metrics_enabled) {
        // start the operating unit resource tracker
        common::thread_context.resource_tracker_.Start();
    }

    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    const auto portal_name = reader_.ReadString();
    const auto statement_name = reader_.ReadString();
    const auto statement = postgres_interpreter->GetStatement(statement_name);

    if (statement == nullptr) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "Statement name referenced by Bind message does not exist.",
                            common::ErrorCode::ERRCODE_INVALID_SQL_STATEMENT_NAME});
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to bind fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        return Transition::PROCEED;
    }

    const auto query_type = statement->GetQueryType();

    if (!portal_name.empty() && postgres_interpreter->GetPortal(statement_name) != nullptr) {
        writer->WriteError(
            {common::ErrorSeverity::ERROR,
             "Named portals must be explicitly closed before they can be redefined by another Bind message.",
             common::ErrorCode::ERRCODE_PROTOCOL_VIOLATION});
        if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
            // failing to parse fails a transaction in postgres
            connection->Transaction()->SetMustAbort();
        }
        return Transition::PROCEED;
    }

    // read out the parameter formats
    const auto param_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&reader_));
    NOISEPAGE_ASSERT(param_formats.size() == 1 || param_formats.size() == statement->ParamTypes().size(),
                     "Incorrect number of parameter format codes. Should either be 1 (all the same) or the number of "
                     "parameters required for this statement.");
    // TODO(Matt): probably shouldn't be an assert, but rather an error response

    // read the params
    auto params
        = PostgresPacketUtil::ReadParameters(common::ManagedPointer(&reader_), statement->ParamTypes(), param_formats);
    uint64_t param_num = params.size();

    // read out the result formats
    auto result_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&reader_));
    // TODO(Matt): would like to assert here that this is 0 (all text), 1 (all the same), or the number of output
    // columns but we can't do that without an OutputSchema yet this early in the pipeline

    // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
    if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL
        && query_type != QueryType::QUERY_COMMIT && query_type != QueryType::QUERY_ROLLBACK) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "current transaction is aborted, commands ignored until end of transaction block",
                            common::ErrorCode::ERRCODE_IN_FAILED_SQL_TRANSACTION});
        return Transition::PROCEED;
    }

    // Begin a transaction, regardless of statement type. If it's a BEGIN statement it's implicitly in this txn
    if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE
        && !SqlUtil::NonTransactionalQueryType(query_type)) {
        NOISEPAGE_ASSERT(!postgres_interpreter->ExplicitTransactionBlock(),
                         "We shouldn't be in an explicit txn block is transaction state is IDLE.");
        taskflow->BeginTransaction(connection);
    }

    if (SqlUtil::TransactionalQueryType(query_type) || SqlUtil::SkipBindQueryType(query_type)) {
        // Don't bind or optimize this statement
        postgres_interpreter->SetPortal(
            portal_name,
            std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
        writer->WriteBindComplete();
        return Transition::PROCEED;
    }

    if (SqlUtil::UnsupportedQueryType(query_type)) {
        // Don't begin an implicit txn in this case, and don't bind or optimize this statement. Just noop with a Notice
        // (not an Error) and proceed to reading more messages.
        postgres_interpreter->SetPortal(
            portal_name,
            std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
        writer->WriteError({common::ErrorSeverity::NOTICE,
                            "we don't yet support that query type.",
                            common::ErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED});
        writer->WriteBindComplete();
        return Transition::PROCEED;
    }

    if (UNLIKELY(SqlUtil::DDLQueryType(query_type))) {
        statement->ClearCachedObjects();
    }

    // Bind it, plan it
    const auto bind_result = taskflow->BindQuery(connection, statement, common::ManagedPointer(&params));
    if (LIKELY(bind_result.type_ == taskflow::ResultType::COMPLETE)) {
        // Binding succeeded, optimize to generate a physical plan
        if (statement->OptimizeResult() == nullptr || !taskflow->UseQueryCache()) {
            // it's not cached, optimize it
            auto optimize_result
                = taskflow->OptimizeBoundQuery(connection, statement->ParseResult(), common::ManagedPointer(&params));

            statement->SetOptimizeResult(std::move(optimize_result));
        }

        postgres_interpreter->SetPortal(
            portal_name,
            std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
        writer->WriteBindComplete();
    } else if (UNLIKELY(bind_result.type_ == taskflow::ResultType::NOTICE)) {
        // Binding generated a NOTICE, i.e. IF EXISTS failed, so we're not going to generate a physical plan of nullptr
        // and handle that case in Execute. In case it previously bound and compiled, we're gonna throw that away for
        // next execution
        statement->ClearCachedObjects();
        NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(bind_result.extra_),
                         "We're expecting a message here.");
        postgres_interpreter->SetPortal(
            portal_name,
            std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
        writer->WriteError(std::get<common::ErrorData>(bind_result.extra_));
        writer->WriteBindComplete();
    } else {
        NOISEPAGE_ASSERT(bind_result.type_ == taskflow::ResultType::ERROR,
                         "I don't think we expect any other ResultType at this point.");
        NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(bind_result.extra_),
                         "We're expecting a message here.");
        // failing to bind fails a transaction in postgres
        connection->Transaction()->SetMustAbort();
        // clear anything cached related to this statement
        statement->ClearCachedObjects();
        writer->WriteError(std::get<common::ErrorData>(bind_result.extra_));
        postgres_interpreter->SetWaitingForSync();
    }

    if (bind_command_metrics_enabled) {
        common::thread_context.resource_tracker_.Stop();
        auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
        common::thread_context.metrics_store_->RecordBindCommandData(param_num,
                                                                     statement->GetQueryText().size(),
                                                                     resource_metrics);
    }

    return Transition::PROCEED;
}

auto DescribeCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                           const common::ManagedPointer<PostgresPacketWriter> writer,
                           const common::ManagedPointer<taskflow::Taskflow> /*taskflow*/,
                           const common::ManagedPointer<ConnectionContext> /*connection*/) -> Transition {
    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    const auto object_type = reader_.ReadValue<DescribeCommandObjectType>();
    const auto object_name = reader_.ReadString();

    if (object_type == DescribeCommandObjectType::PORTAL) {
        const auto portal = postgres_interpreter->GetPortal(object_name);
        if (portal == nullptr) {
            writer->WriteError({common::ErrorSeverity::ERROR,
                                "Portal does not exist for Describe message.",
                                common::ErrorCode::ERRCODE_PROTOCOL_VIOLATION});
        } else if (portal->GetStatement()->GetQueryType() == network::QueryType::QUERY_SELECT) {
            writer->WriteRowDescription(portal->OptimizeResult()->GetPlanNode()->GetOutputSchema()->GetColumns(),
                                        portal->ResultFormats());
        } else if (portal->GetStatement()->GetQueryType() == network::QueryType::QUERY_EXPLAIN) {
            writer->WriteExplainRowDescription();
        } else if (portal->GetStatement()->GetQueryType() == network::QueryType::QUERY_SHOW) {
            WriteShowRowDescription(writer, portal->GetStatement());
        } else {
            writer->WriteNoData();
        }
        return Transition::PROCEED;
    }
    NOISEPAGE_ASSERT(object_type == DescribeCommandObjectType::STATEMENT,
                     "Unknown object type passed in Close message.");

    const auto statement = postgres_interpreter->GetStatement(object_name);

    if (statement == nullptr) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "Statement does not exist for Describe message.",
                            common::ErrorCode::ERRCODE_INVALID_SQL_STATEMENT_NAME});
    } else {
        writer->WriteParameterDescription(statement->ParamTypes());
        if (statement->GetQueryType() == network::QueryType::QUERY_SELECT) {
            // TODO(Matt): we're supposed to write a RowDescription here, but we don't have an OutputSchema yet because
            // that comes all the way after optimization
        } else {
            writer->WriteNoData();
        }
    }

    return Transition::PROCEED;
}

auto ExecuteCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                          const common::ManagedPointer<PostgresPacketWriter> writer,
                          const common::ManagedPointer<taskflow::Taskflow>   taskflow,
                          const common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    const bool execute_command_metrics_enabled
        = common::thread_context.metrics_store_ != nullptr
          && common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::EXECUTE_COMMAND);
    if (execute_command_metrics_enabled) {
        // start the operating unit resource tracker
        common::thread_context.resource_tracker_.Start();
    }

    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    const auto          portal_name = reader_.ReadString();
    const auto max_rows UNUSED_ATTRIBUTE = reader_.ReadValue<int32_t>();
    // TODO(Matt): here's where you would reason about PortalSuspend with max_rows. Maybe just the OutputWriter would
    // buffer the results, since I'm not sure if we can actually pause execution engine

    const auto portal = postgres_interpreter->GetPortal(portal_name);

    if (portal == nullptr) {
        writer->WriteError({common::ErrorSeverity::ERROR,
                            "Specified portal does not exist to execute.",
                            common::ErrorCode::ERRCODE_PROTOCOL_VIOLATION});
        return Transition::PROCEED;
    }

    const auto statement = portal->GetStatement();
    const auto query_type = statement->GetQueryType();

    // TODO(Matt): Probably handle EmptyStatement around here somewhere, maybe somewhere more general purpose though?
    // Set statements are manually handled here.
    if (UNLIKELY(query_type == network::QueryType::QUERY_SET)) {
        if (postgres_interpreter->ExplicitTransactionBlock()) {
            writer->WriteError({common::ErrorSeverity::ERROR,
                                "SET cannot run inside a transaction block",
                                common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
            connection->Transaction()->SetMustAbort();
            return FinishSimpleQueryCommand(writer, connection);
        }

        auto set_result = taskflow->ExecuteSetStatement(connection, common::ManagedPointer(statement));
        if (set_result.type_ == taskflow::ResultType::ERROR) {
            writer->WriteError(std::get<common::ErrorData>(set_result.extra_));
        } else {
            writer->WriteCommandComplete(network::QueryType::QUERY_SET, 0);
        }
        return Transition::PROCEED;
    }

    // Show statements are manually handled here.
    if (UNLIKELY(query_type == network::QueryType::QUERY_SHOW)) {
        auto show_result = taskflow->ExecuteShowStatement(connection, writer, portal->GetStatement());
        NOISEPAGE_ASSERT(show_result.type_ == taskflow::ResultType::COMPLETE,
                         "TODO(WAN) this should be fixed to handle failure.");
        writer->WriteCommandComplete(query_type, std::get<uint32_t>(show_result.extra_));
        return Transition::PROCEED;
    }

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (SqlUtil::TransactionalQueryType(query_type)) {
        taskflow->ExecuteTransactionStatement(connection,
                                              writer,
                                              postgres_interpreter->ExplicitTransactionBlock(),
                                              query_type);
        if (query_type == network::QueryType::QUERY_BEGIN) {
            if (!(postgres_interpreter->ExplicitTransactionBlock())) {
                postgres_interpreter->SetExplicitTransactionBlock();
            }
        } else {
            postgres_interpreter->ResetTransactionState();
        }
        return Transition::PROCEED;
    }

    if (SqlUtil::UnsupportedQueryType(query_type)) {
        // We don't yet support query types with values greater than this
        writer->WriteCommandComplete(query_type, 0);
        return Transition::PROCEED;
    }

    if (execute_command_metrics_enabled) {
        common::thread_context.resource_tracker_.Stop();
        auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
        common::thread_context.metrics_store_->RecordExecuteCommandData(portal_name.size(), resource_metrics);
    }

    if (portal->OptimizeResult() != nullptr) {
        ExecutePortal(connection, portal, writer, taskflow, postgres_interpreter->ExplicitTransactionBlock());
        if (connection->TransactionState() == NetworkTransactionStateType::FAIL) {
            postgres_interpreter->SetWaitingForSync();
        }
    } else {
        // This happens in the event of a noop generated earlier (like in binding with an IF EXISTS);
        writer->WriteCommandComplete(query_type, 0);
    }

    return Transition::PROCEED;
}

auto SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter>  interpreter,
                       common::ManagedPointer<PostgresPacketWriter> writer,
                       common::ManagedPointer<taskflow::Taskflow>   taskflow,
                       common::ManagedPointer<ConnectionContext>    connection) -> Transition {
    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    if (!postgres_interpreter->ExplicitTransactionBlock()
        && !(connection->TransactionState() == network::NetworkTransactionStateType::IDLE)) {
        taskflow->EndTransaction(connection,
                                 connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                        : network::QueryType::QUERY_COMMIT);
        postgres_interpreter->ResetTransactionState();
    } else if (postgres_interpreter->WaitingForSync()) {
        postgres_interpreter->ResetWaitingForSync();
    }
    writer->WriteReadyForQuery(connection->TransactionState());
    return Transition::PROCEED;
}

auto CloseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter>  interpreter,
                        const common::ManagedPointer<PostgresPacketWriter> writer,
                        const common::ManagedPointer<taskflow::Taskflow> /*taskflow*/,
                        const common::ManagedPointer<ConnectionContext> /*connection*/) -> Transition {
    const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
    NOISEPAGE_ASSERT(!postgres_interpreter->WaitingForSync(),
                     "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                     "caught at the protocol interpreter Process() level.");

    const auto object_type = reader_.ReadValue<DescribeCommandObjectType>();
    const auto object_name = reader_.ReadString();
    if (object_type == DescribeCommandObjectType::PORTAL) {
        postgres_interpreter->ClosePortal(object_name);
        // "It is not an error to issue Close against a nonexistent statement or portal name."
        writer->WriteCloseComplete();
        return Transition::PROCEED;
    }
    NOISEPAGE_ASSERT(object_type == DescribeCommandObjectType::STATEMENT,
                     "Unknown object type passed in Close message.");
    postgres_interpreter->CloseStatement(object_name);
    // "It is not an error to issue Close against a nonexistent statement or portal name."
    writer->WriteCloseComplete();
    return Transition::PROCEED;
}

auto TerminateCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> /*interpreter*/,
                            const common::ManagedPointer<PostgresPacketWriter> /*out*/,
                            const common::ManagedPointer<taskflow::Taskflow> /*taskflow*/,
                            const common::ManagedPointer<ConnectionContext> /*connection*/) -> Transition {
    // Postgres doesn't send any sort of response for the Terminate command
    // We don't do removal of the temp namespace at the Command level because it's possible that we don't receive a
    // Terminate packet to generate the Command from, and instead closed the connection due to timeout
    return Transition::TERMINATE;
}

// (Matt): this seems to only exist for testing
auto EmptyCommand::Exec(common::ManagedPointer<ProtocolInterpreter> /*interpreter*/,
                        common::ManagedPointer<PostgresPacketWriter> writer,
                        common::ManagedPointer<taskflow::Taskflow> /*taskflow*/,
                        common::ManagedPointer<ConnectionContext> /*connection*/) -> Transition {
    NETWORK_LOG_TRACE("Empty Command");
    writer->WriteEmptyQueryResponse();
    writer->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
}
} // namespace noisepage::network
