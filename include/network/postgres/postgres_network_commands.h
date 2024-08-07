#pragma once
#include "network/network_command.h"

namespace noisepage::network {

/**
 * Interface for the execution of the standard PostgresNetworkCommands for the postgres protocol. Any state/logic that
 * is Postgres command-specific should live at this layer.
 */
class PostgresNetworkCommand : public NetworkCommand {
public:
    /**
     * Executes the command
     * @param interpreter The protocol interpreter that called this
     * @param out The Writer on which to construct output packets for the client
     * @param taskflow The taskflow pointer
     * @param connection The ConnectionContext which contains connection information
     * @return The next transition for the client's state machine
     */
    virtual auto Exec(common::ManagedPointer<ProtocolInterpreter>  interpreter,
                      common::ManagedPointer<PostgresPacketWriter> writer,
                      common::ManagedPointer<taskflow::Taskflow>   taskflow,
                      common::ManagedPointer<ConnectionContext>    connection) -> Transition
        = 0;

protected:
    /**
     * Constructor for a PostgresNetworkCommand instance
     * @param in The input packets to this command
     * @param flush Whether or not to flush the output packets on completion
     */
    PostgresNetworkCommand(const common::ManagedPointer<InputPacket> in, bool flush)
        : NetworkCommand(in, flush) {}
};

#define DEFINE_POSTGRES_COMMAND(COMMAND, flush)                                                                        \
    class COMMAND : public PostgresNetworkCommand {                                                                    \
    public:                                                                                                            \
        explicit COMMAND(const common::ManagedPointer<InputPacket> in)                                                 \
            : PostgresNetworkCommand(in, flush) {}                                                                     \
        auto Exec(common::ManagedPointer<ProtocolInterpreter>  interpreter,                                            \
                  common::ManagedPointer<PostgresPacketWriter> writer,                                                 \
                  common::ManagedPointer<taskflow::Taskflow>   taskflow,                                               \
                  common::ManagedPointer<ConnectionContext>    connection) -> Transition override;                        \
    }

// Set all to force flush for now
DEFINE_POSTGRES_COMMAND(SimpleQueryCommand, true);
DEFINE_POSTGRES_COMMAND(ParseCommand, false);
DEFINE_POSTGRES_COMMAND(BindCommand, false);
DEFINE_POSTGRES_COMMAND(DescribeCommand, false);
DEFINE_POSTGRES_COMMAND(ExecuteCommand, false);
DEFINE_POSTGRES_COMMAND(SyncCommand, true);
DEFINE_POSTGRES_COMMAND(CloseCommand, true);
DEFINE_POSTGRES_COMMAND(TerminateCommand, true);
DEFINE_POSTGRES_COMMAND(EmptyCommand, true); // (Matt): This seems to be only for testing? Not a big fan of that.

#undef DEFINE_POSTGRES_COMMAND

} // namespace noisepage::network
