#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/group_expression.h"
#include "optimizer/physical_operators.h"
#include "transaction/transaction_context.h"

namespace noisepage {
namespace optimizer {

    class Memo;
    class GroupExpression;

    /**
     * This cost model is meant to just be a trivial cost model. The decisions it makes are as follows
     * Always choose index scan (cost of 0) over sequential scan (cost of 1)
     * Choose NL if left rows is a single record (for single record lookup queries), else choose hash join
     * Choose hash group by over sort group by
     */
    class ForcedCostModel : public AbstractCostModel {
    public:
        /**
         * Default constructor
         * @param pick_hash_join Whether to pick hash join
         */
        explicit ForcedCostModel(bool pick_hash_join)
            : pick_hash_join_(pick_hash_join) {}

        /**
         * Costs a GroupExpression
         * @param txn TransactionContext that query is generated under
         * @param accessor CatalogAccessor
         * @param memo Memo object containing all relevant groups
         * @param gexpr GroupExpression to calculate cost for
         */
        double CalculateCost(transaction::TransactionContext           *txn,
                             [[maybe_unused]] catalog::CatalogAccessor *accessor,
                             Memo                                      *memo,
                             GroupExpression                           *gexpr) override {
            gexpr_ = gexpr;
            memo_ = memo;
            txn_ = txn;
            gexpr_->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
            return output_cost_;
        };

        /**
         * Visit a SeqScan operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const SeqScan *op) override {
            output_cost_ = 1.f;
        }

        /**
         * Visit a IndexScan operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const IndexScan *op) override {
            output_cost_ = 0.f;
        }

        /**
         * Visit a QueryDerivedScan operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const QueryDerivedScan *op) override {
            output_cost_ = 0.f;
        }

        /**
         * Visit a OrderBy operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const OrderBy *op) override {
            output_cost_ = 0.f;
        }

        /**
         * Visit a Limit operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const Limit *op) override {
            output_cost_ = 0.f;
        }

        /**
         * Visit a InnerNLJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const InnerNLJoin *op) override {
            output_cost_ = (pick_hash_join_) ? 1.f : 0.f;
        }

        /**
         * Visit a LeftNLJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const LeftNLJoin *op) override {}

        /**
         * Visit a RightNLJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const RightNLJoin *op) override {}

        /**
         * Visit a OuterNLJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const OuterNLJoin *op) override {}

        /**
         * Visit a InnerHashJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const InnerHashJoin *op) override {
            output_cost_ = (pick_hash_join_) ? 0.f : 1.f;
        }

        /**
         * Visit a LeftHashJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const LeftHashJoin *op) override {}

        /**
         * Visit a RightHashJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const RightHashJoin *op) override {}

        /**
         * Visit a OuterHashJoin operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const OuterHashJoin *op) override {}

        /**
         * Visit a Insert operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const Insert *op) override {}

        /**
         * Visit a InsertSelect operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const InsertSelect *op) override {}

        /**
         * Visit a Delete operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const Delete *op) override {}

        /**
         * Visit a Update operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const Update *op) override {}

        /**
         * Visit a HashGroupBy operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const HashGroupBy *op) override {
            output_cost_ = 0.f;
        }

        /**
         * Visit a SortGroupBy operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const SortGroupBy *op) override {
            output_cost_ = 1.f;
        }

        /**
         * Visit a Aggregate operator
         * @param op operator
         */
        void Visit([[maybe_unused]] const Aggregate *op) override {
            output_cost_ = 0.f;
        }

    private:
        /**
         * GroupExpression to cost
         */
        GroupExpression *gexpr_;

        /**
         * Memo table to use
         */
        Memo *memo_;

        /**
         * Transaction Context
         */
        transaction::TransactionContext *txn_;

        /**
         * Computed output cost
         */
        double output_cost_ = 0;

        /**
         * Should pick hash join
         */
        bool pick_hash_join_ = false;
    };

} // namespace optimizer
} // namespace noisepage
