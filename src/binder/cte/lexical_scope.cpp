#include "binder/cte/lexical_scope.h"

#include <algorithm>

#include "binder/cte/context_sensitive_table_ref.h"
#include "common/macros.h"
#include "parser/table_ref.h"

namespace noisepage::binder::cte {

LexicalScope::LexicalScope(std::size_t id, std::size_t depth, const LexicalScope *enclosing_scope)
    : id_{id}
    , depth_{depth}
    , enclosing_scope_{enclosing_scope} {}

auto LexicalScope::RefCount() const -> std::size_t {
    return references_.size();
}

auto LexicalScope::ReadRefCount() const -> std::size_t {
    return RefCountWithType(RefType::READ);
}

auto LexicalScope::WriteRefCount() const -> std::size_t {
    return RefCountWithType(RefType::WRITE);
}

auto LexicalScope::RefCountWithType(RefType type) const -> std::size_t {
    return std::count_if(references_.cbegin(),
                         references_.cend(),
                         [=](const std::unique_ptr<ContextSensitiveTableRef> &r) {
                             return r->Type() == type;
                         });
}

auto LexicalScope::References() -> std::vector<std::unique_ptr<ContextSensitiveTableRef>> & {
    return references_;
}

auto LexicalScope::References() const -> const std::vector<std::unique_ptr<ContextSensitiveTableRef>> & {
    return references_;
}

void LexicalScope::AddEnclosedScope(std::unique_ptr<LexicalScope> &&scope) {
    enclosed_scopes_.push_back(std::move(scope));
}

void LexicalScope::AddReference(std::unique_ptr<ContextSensitiveTableRef> &&ref) {
    references_.push_back(std::move(ref));
}

auto LexicalScope::PositionOf(std::string_view alias, RefType type) const -> std::size_t {
    auto it = std::find_if(references_.cbegin(),
                           references_.cend(),
                           [&](const std::unique_ptr<ContextSensitiveTableRef> &ref) {
                               return ref->Type() == type && ref->Table()->GetAlias().GetName() == alias;
                           });
    NOISEPAGE_ASSERT(it != references_.cend(), "Requested table reference not present in scope");
    return std::distance(references_.cbegin(), it);
}

} // namespace noisepage::binder::cte
