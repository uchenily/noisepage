#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

class ActionState;

/**
 * The abstract class for self-driving actions
 */
class AbstractAction {
public:
    /**
     * Constructor for the base AbstractAction.
     * @param family The family that this action belongs to
     * @param db_oid The ID of the database that this action belongs to
     */
    explicit AbstractAction(ActionType family, catalog::db_oid_t db_oid)
        : id_(action_id_counter++)
        , action_type_(family)
        , db_oid_(db_oid) {}

    virtual ~AbstractAction() = default;

    /**
     * Set the estimated runtime metrics for this action
     * @param estimated_metrics The metrics to set to
     */
    void SetEstimatedMetrics(std::vector<double> &&estimated_metrics) {
        estimated_metrics_ = std::move(estimated_metrics);
    }

    /** @return The estimated runtime metrics for this action */
    const std::vector<double> &GetEstimatedMetrics() {
        return estimated_metrics_;
    }

    /** @return The estimated elapsed time in us for this action */
    double GetEstimatedElapsedUs() {
        if (estimated_metrics_.empty())
            return 0;
        // Assumes the elapsed time is the last element
        return estimated_metrics_.end()[-1];
    }

    /** @return The estimated memory consumption in bytes for this action */
    double GetEstimatedMemoryBytes() {
        if (estimated_metrics_.size() < 2)
            return 0;
        // Assumes the memory consumption is the second last element
        return estimated_metrics_.end()[-2];
    }

    /** @return This action's ID */
    action_id_t GetActionID() const {
        return id_;
    }

    /** @return This action's type */
    ActionType GetActionType() const {
        return action_type_;
    }

    /** @return This action's database oid */
    catalog::db_oid_t GetDatabaseOid() const {
        return db_oid_;
    }

    /**
     * Add an invalidated action
     * @param id Action ID
     */
    void AddInvalidatedAction(action_id_t id) {
        invalidated_action_ids_.emplace_back(id);
    }

    /**
     * Get the invalidated action ids if this action is applied
     * @return Action ID vector
     */
    const std::vector<action_id_t> &GetInvalidatedActions() const {
        return invalidated_action_ids_;
    }

    /**
     * Add an invalidated action
     * @param id Action ID
     */
    void AddEnabledAction(action_id_t id) {
        enabled_action_ids_.emplace_back(id);
    }

    /**
     * Get the enabled action ids if this action is applied
     * @return Action ID vector
     */
    const std::vector<action_id_t> &GetEnabledActions() const {
        return enabled_action_ids_;
    }

    /**
     * Add a reverse action
     * @param id Action ID
     */
    void AddReverseAction(action_id_t id) {
        reverse_action_ids_.emplace_back(id);
    }

    /**
     * Get the reverse action ids
     * @return Action ID vector
     */
    const std::vector<action_id_t> &GetReverseActions() const {
        return reverse_action_ids_;
    }

    /**
     * Get the SQL command to apply the action
     * @return Action SQL command
     */
    virtual const std::string &GetSQLCommand() {
        return sql_command_;
    }

    /**
     * Check whether the action is valid to apply.
     * Possible scenarios that the action is invalid to apply: the knob setting is out of the valid range, the index
     * requires more memory than available in the system, etc.
     * TODO(lin): add the available memory as input param
     * @return true if the action is valid to apply, false otherwise
     */
    virtual bool IsValid() {
        return true;
    }

    /**
     * Modify the action state given the effect of this action
     * @param action_state the action state to modify
     */
    virtual void ModifyActionState(ActionState *action_state) = 0;

protected:
    /** ID is unique for an action among on planning process (one MCTS) */
    action_id_t id_;

    std::string sql_command_; ///< The SQL commaned used to apply the action

private:
    static action_id_t action_id_counter;

    std::vector<double> estimated_metrics_;

    ActionType action_type_;

    catalog::db_oid_t db_oid_;

    std::vector<action_id_t> invalidated_action_ids_;
    std::vector<action_id_t> enabled_action_ids_;
    std::vector<action_id_t> reverse_action_ids_;
};

} // namespace noisepage::selfdriving::pilot
