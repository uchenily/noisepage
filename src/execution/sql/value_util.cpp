#include "execution/sql/value_util.h"

#include "common/allocator.h"
#include "execution/sql/value.h"

namespace noisepage::execution::sql {

auto ValueUtil::CreateStringVal(const common::ManagedPointer<const char> string, const uint32_t length)
    -> std::pair<StringVal, std::unique_ptr<byte[]>> {
    if (length <= StringVal::InlineThreshold()) {
        return {StringVal(string.Get(), length), nullptr};
    }
    // TODO(Matt): smarter allocation?
    auto buffer = std::unique_ptr<byte[]>(common::AllocationUtil::AllocateAligned(length));
    std::memcpy(buffer.get(), string.Get(), length);
    return {StringVal(reinterpret_cast<const char *>(buffer.get()), length), std::move(buffer)};
}

auto ValueUtil::CreateStringVal(const std::string &string) -> std::pair<StringVal, std::unique_ptr<byte[]>> {
    return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

auto ValueUtil::CreateStringVal(const std::string_view string) -> std::pair<StringVal, std::unique_ptr<byte[]>> {
    return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

auto ValueUtil::CreateStringVal(const StringVal string) -> std::pair<StringVal, std::unique_ptr<byte[]>> {
    return CreateStringVal(common::ManagedPointer(string.GetContent()), string.GetLength());
}

} // namespace noisepage::execution::sql
