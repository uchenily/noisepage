#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/mcts/action_state.h"

#define EPSILON 1e-3
#define NULL_ACTION INT32_MAX

namespace noisepage::selfdriving {
class WorkloadForecast;

namespace pilot {
    class Pilot;
    class PlanningContext;

    STRONG_TYPEDEF_HEADER(tree_node_id_t, uint64_t);

    constexpr tree_node_id_t INVALID_TREE_NODE_ID = tree_node_id_t(0);

    /**
     * The pilot processes the query trace predictions by executing them and extracting pipeline features
     */
    class TreeNode {
    public:
        /**
         * Constructor for tree node
         * @param parent pointer to parent
         * @param current_action action that leads its parent to the current node, root has NULL action
         * @param current_segment_cost cost of executing current segment with actions applied on path from root to
         * current node
         * @param action_start_segment_index start of segment index that this node will influence
         * @param later_segments_cost cost of later segments when actions applied on path from root to current node
         * @param memory memory consumption at the current node in bytes
         * @param action_state the state of the action after the intervals represented by this node.
         */
        TreeNode(common::ManagedPointer<TreeNode> parent,
                 action_id_t                      current_action,
                 uint64_t                         action_start_segment_index,
                 double                           current_segment_cost,
                 double                           later_segments_cost,
                 uint64_t                         memory,
                 ActionState                      action_state);

        /**
         * @return action id at node with least cost
         */
        common::ManagedPointer<TreeNode> BestSubtree();

        /**
         * @return nodes ordered from optimal to least optimal
         */
        std::vector<common::ManagedPointer<TreeNode>> BestSubtreeOrdering();

        /**
         * @return depth of the treenode in the search tree
         */
        uint64_t GetDepth() {
            return depth_;
        }

        /**
         * Recursively sample the vertex whose children will be assigned values through rollout.
         * @param root pointer to root of the search tree
         * @param planning_context pilot planning context
         * @param action_map action map of the search tree
         * @param candidate_actions candidate actions that can be applied at curent node
         * @param end_segment_index last segment index to be considered in forecast (needed so that when sampled leaf is
         * beyond this index, we repeat the selection process)
         */
        static common::ManagedPointer<TreeNode>
        Selection(common::ManagedPointer<TreeNode>                              root,
                  const PlanningContext                                        &planning_context,
                  const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                  std::unordered_set<action_id_t>                              *candidate_actions,
                  uint64_t                                                      end_segment_index);

        /**
         * Expand each child of current node and update its cost and num of visits accordingly
         * @param planning_context pilot planning context
         * @param forecast pointer to forecasted workload
         * @param action_horizon number of next levels only influenced by the action selected at current node
         * @param tree_end_segment_index end_segment_index of the search tree
         * @param action_map action map of the search tree
         * @param candidate_actions candidate actions of the search tree
         * @param action_state_cost_map caches the previous rollout cost calculation based on the action state
         * @param action_apply_cost_map caches the previous rollout cost calculation based on the <action state, action>
         * pair
         * @param memory_constraint maximum allowed memory in bytes
         */
        void ChildrenRollout(PlanningContext                                              *planning_context,
                             common::ManagedPointer<WorkloadForecast>                      forecast,
                             uint64_t                                                      action_horizon,
                             uint64_t                                                      tree_end_segment_index,
                             const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                             const std::unordered_set<action_id_t>                        &candidate_actions,
                             std::unordered_map<ActionState, double, ActionStateHasher>   *action_state_cost_map,
                             std::unordered_map<std::pair<ActionState, action_id_t>,
                                                std::pair<double, uint64_t>,
                                                ActionStateActionPairHasher>              *action_apply_cost_map,
                             uint64_t                                                      memory_constraint);

        /**
         * Update the visits number and cost of the node and its ancestors in tree due to expansion of its children,
         * also apply reverse actions
         * @param planning_context pilot planning context
         * @param action_map action map of the search tree
         * @param use_min_cost whether to use the minimum cost of all leaves as the cost for internal nodes
         */
        void BackPropogate(const PlanningContext                                        &planning_context,
                           const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                           bool                                                          use_min_cost);

        /**
         * Return if current node is a leaf
         * @return is_leaf_
         */
        bool IsLeaf() {
            return is_leaf_;
        }

        /**
         * Get action taken to get to this node from its parent
         * @return current action
         */
        action_id_t GetCurrentAction() {
            return current_action_;
        }

        /**
         * Get estimated cost of applying the action
         * @return estimated cost
         */
        double GetCost() {
            return cost_;
        }

        /**
         * @return tree node id
         */
        tree_node_id_t GetTreeNodeId() {
            return tree_node_id_;
        }

        /**
         * @return action start segment index
         */
        uint64_t GetActionStartSegmentIndex() {
            return action_start_segment_index_;
        }

        /**
         * @return action plan end index
         */
        uint64_t GetActionPlanEndIndex() {
            return action_plan_end_index_;
        }

    private:
        /**
         * Sample child based on cost and number of visits
         * @return pointer to sampled child
         */
        common::ManagedPointer<TreeNode> SampleChild();

        /**
         * Compute cost as average of children weighted by num of visits of each one
         * (usually only used on the leaf being expanded in a simulation round, for nonleaf see UpdateCostAndVisits)
         * @return recomputed cost of current node
         */
        double ComputeWeightedAverageCostFromChildren() {
            uint64_t child_sum = 0, total_visits = 0;
            for (auto &child : children_) {
                child_sum += child->cost_ * child->number_of_visits_;
                total_visits += child->number_of_visits_;
            }
            NOISEPAGE_ASSERT(total_visits > 0, "num visit of the subtree rooted at a node cannot be zero");
            return child_sum / (total_visits + EPSILON);
        }

        /**
         * Compute cost as the minimum of all children
         * (usually only used on the leaf being expanded in a simulation round, for nonleaf see UpdateCostAndVisits)
         * @return recomputed cost of current node
         */
        double ComputeMinCostFromChildren() {
            NOISEPAGE_ASSERT(!children_.empty(), "number of children cannot be zero");
            double min_cost = children_[0]->cost_;
            for (auto &child : children_)
                min_cost = std::min(min_cost, child->cost_);
            return min_cost;
        }

        /**
         * Update number of visits to the current node aka number of tree traversals to a leaf
         * containing the path to current node, and the cost of the node; based on the expansion of a successor as a
         * leaf
         * @param num_expansion number of children of the expanded leaf
         * @param leaf_cost previous cost of the leaf
         * @param expanded_cost new cost of the leaf after expansion
         */
        void UpdateCostAndVisits(uint64_t num_expansion, double leaf_cost, double expanded_cost);

        double ComputeCostWithAction(PlanningContext                         *planning_context,
                                     common::ManagedPointer<WorkloadForecast> forecast,
                                     uint64_t                                 tree_end_segment_index,
                                     AbstractAction                          *action);

        static constexpr double MEMORY_CONSUMPTION_VIOLATION_COST = 1e10;

        tree_node_id_t    tree_node_id_;
        bool              is_leaf_;
        const uint64_t    depth_;                      // number of edges in path from root
        const uint64_t    action_start_segment_index_; // start of segment index that this node will influence
        uint64_t          action_plan_end_index_;      // end of segment index for planning
        const action_id_t current_action_;
        const double
            ancestor_cost_; // cost of executing segments with actions applied on path from root to current node
        const common::ManagedPointer<TreeNode> parent_;

        uint64_t                               number_of_visits_; // number of leaf in subtree rooted at node
        std::vector<std::unique_ptr<TreeNode>> children_;
        double                                 cost_;
        uint64_t                               memory_;
        ActionState                            action_state_;

        static tree_node_id_t tree_node_identifier;
    };
} // namespace pilot

} // namespace noisepage::selfdriving
