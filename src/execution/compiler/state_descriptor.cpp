#include "execution/compiler/state_descriptor.h"

#include <utility>

#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"

namespace noisepage::execution::compiler {

//===----------------------------------------------------------------------===//
//
// State Entry
//
//===----------------------------------------------------------------------===//

auto StateDescriptor::Entry::Get(CodeGen *codegen) const -> ast::Expr * {
    return codegen->AccessStructMember(desc_->GetStatePointer(codegen), member_);
}

auto StateDescriptor::Entry::GetPtr(CodeGen *codegen) const -> ast::Expr * {
    return codegen->AddressOf(Get(codegen));
}

auto StateDescriptor::Entry::OffsetFromState(CodeGen *codegen) const -> ast::Expr * {
    return codegen->OffsetOf(desc_->GetType()->Name(), member_);
}

//===----------------------------------------------------------------------===//
//
// State Descriptor
//
//===----------------------------------------------------------------------===//

StateDescriptor::StateDescriptor(ast::Identifier name, StateDescriptor::InstanceProvider access)
    : name_(name)
    , access_(std::move(access))
    , state_type_(nullptr) {}

auto StateDescriptor::DeclareStateEntry(CodeGen *codegen, const std::string &name, ast::Expr *type_repr)
    -> StateDescriptor::Entry {
    NOISEPAGE_ASSERT(state_type_ == nullptr, "Cannot add to state after it's been finalized");
    ast::Identifier member = codegen->MakeFreshIdentifier(name);
    slots_.emplace_back(member, type_repr);
    return Entry(this, member);
}

auto StateDescriptor::ConstructFinalType(CodeGen *codegen) -> ast::StructDecl * {
    // Early exit if the state is already constructed.
    if (state_type_ != nullptr) {
        return state_type_;
    }

    // Collect fields and build the structure type.
    util::RegionVector<ast::FieldDecl *> fields = codegen->MakeEmptyFieldList();
    for (auto &slot : slots_) {
        fields.push_back(codegen->MakeField(slot.name_, slot.type_repr_));
    }
    state_type_ = codegen->DeclareStruct(name_, std::move(fields));

    // Done
    return state_type_;
}

auto StateDescriptor::GetSize() const -> std::size_t {
    NOISEPAGE_ASSERT(state_type_ != nullptr, "State has not been constructed");
    NOISEPAGE_ASSERT(state_type_->TypeRepr()->GetType() != nullptr, "Type-checking not completed!");
    return state_type_->TypeRepr()->GetType()->GetSize();
}

} // namespace noisepage::execution::compiler
