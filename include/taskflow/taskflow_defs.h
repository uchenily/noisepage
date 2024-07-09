#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "common/error/error_data.h"

namespace noisepage::taskflow {

/**
 * Prefix of per connection temporary namespaces
 */
static constexpr std::string_view TEMP_NAMESPACE_PREFIX = "pg_temp_";

enum class ResultType : uint8_t { COMPLETE, ERROR, NOTICE, NOOP, QUEUING, UNKNOWN };

/**
 * Standardized return value for Taskflow operations.
 */
struct TaskflowResult {
    /**
     * Classifies the result to help decide what to do with the result
     */
    ResultType type_ = ResultType::UNKNOWN;
    /**
     * The number is envisioned for operations that return a number (INSERT, UPDATE, DELETE) or the string can be used
     * to return error messages.
     */
    std::variant<uint32_t, common::ErrorData> extra_;
};

} // namespace noisepage::taskflow
