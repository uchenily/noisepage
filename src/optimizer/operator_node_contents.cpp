#include "optimizer/operator_node_contents.h"

#include <memory>
#include <string>
#include <utility>

#include "common/managed_pointer.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

Operator::Operator() noexcept = default;

Operator::Operator(common::ManagedPointer<BaseOperatorNodeContents> contents)
    : AbstractOptimizerNodeContents(contents.CastTo<AbstractOptimizerNodeContents>()) {}

Operator::Operator(Operator &&o) noexcept
    : AbstractOptimizerNodeContents(o.contents_) {}

void Operator::Accept(common::ManagedPointer<OperatorVisitor> v) const {
    contents_->Accept(v);
}

auto Operator::GetName() const -> std::string {
    if (IsDefined()) {
        return contents_->GetName();
    }
    return "Undefined";
}

auto Operator::GetOpType() const -> OpType {
    if (IsDefined()) {
        return contents_->GetOpType();
    }

    return OpType::UNDEFINED;
}

auto Operator::GetExpType() const -> parser::ExpressionType {
    return parser::ExpressionType::INVALID;
}

auto Operator::IsLogical() const -> bool {
    if (IsDefined()) {
        return contents_->IsLogical();
    }
    return false;
}

auto Operator::IsPhysical() const -> bool {
    if (IsDefined()) {
        return contents_->IsPhysical();
    }
    return false;
}

auto Operator::Hash() const -> common::hash_t {
    if (IsDefined()) {
        return contents_->Hash();
    }
    return 0;
}

auto Operator::operator==(const Operator &rhs) const -> bool {
    if (IsDefined() && rhs.IsDefined()) {
        return *contents_ == *rhs.contents_;
    }

    return !IsDefined() && !rhs.IsDefined();
}

auto Operator::IsDefined() const -> bool {
    return contents_ != nullptr;
}

auto Operator::RegisterWithTxnContext(transaction::TransactionContext *txn) -> Operator {
    auto *op = dynamic_cast<BaseOperatorNodeContents *>(contents_.Get());
    if (txn != nullptr) {
        txn->RegisterCommitAction([=]() {
            delete op;
        });
        txn->RegisterAbortAction([=]() {
            delete op;
        });
    }
    return *this;
}

} // namespace noisepage::optimizer
