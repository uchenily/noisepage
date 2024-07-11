#include "execution/sema/scope.h"

#include "execution/ast/ast.h"

namespace noisepage::execution::sema {

auto Scope::Declare(ast::Identifier decl_name, ast::Type *type) -> bool {
    ast::Type *curr_decl = Lookup(decl_name);
    if (curr_decl != nullptr) {
        return false;
    }
    decls_[decl_name] = type;
    return true;
}

auto Scope::Lookup(ast::Identifier name) const -> ast::Type * {
    for (const Scope *scope = this; scope != nullptr; scope = scope->Outer()) {
        if (ast::Type *decl_type = scope->LookupLocal(name)) {
            return decl_type;
        }
    }

    // Not in any scope
    return nullptr;
}

auto Scope::LookupLocal(ast::Identifier name) const -> ast::Type * {
    auto iter = decls_.find(name);
    return (iter == decls_.end() ? nullptr : iter->second);
}

} // namespace noisepage::execution::sema
