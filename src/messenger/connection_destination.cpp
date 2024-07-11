#include "messenger/connection_destination.h"

#include "spdlog/fmt/fmt.h"

namespace noisepage::messenger {

auto ConnectionDestination::MakeTCP(std::string target_name, std::string_view hostname, int port)
    -> ConnectionDestination {
    return ConnectionDestination(std::move(target_name), fmt::format("tcp://{}:{}", hostname, port));
}

auto ConnectionDestination::MakeIPC(std::string target_name, std::string_view pathname) -> ConnectionDestination {
    return ConnectionDestination(std::move(target_name), fmt::format("ipc://{}", pathname));
}

auto ConnectionDestination::MakeInProc(std::string target_name, std::string_view endpoint) -> ConnectionDestination {
    return ConnectionDestination(std::move(target_name), fmt::format("inproc://{}", endpoint));
}

} // namespace noisepage::messenger
