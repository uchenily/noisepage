#include "loggers/messenger_logger.h"

#include <memory>

namespace noisepage::messenger {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr messenger_logger = nullptr;

void InitMessengerLogger() {
    if (messenger_logger == nullptr) {
        messenger_logger = std::make_shared<spdlog::logger>("messenger_logger", ::default_sink); // NOLINT
        spdlog::register_logger(messenger_logger);
    }
}
#endif
} // namespace noisepage::messenger
