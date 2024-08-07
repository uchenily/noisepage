#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "execution/sql/sql.h"

namespace noisepage::parser {
class ConstantValueExpression;
class AbstractExpression;
class SelectStatement;
class TableRef;
} // namespace noisepage::parser

namespace noisepage::catalog {
class CatalogAccessor;
} // namespace noisepage::catalog

namespace noisepage::binder {

/**
 * Static utility functions for the binder
 */
class BinderUtil {
public:
    BinderUtil() = delete;

    /**
     * Given a vector of parameters, and their desired types, promote them. This is used to fast-path parameter
     * casting/promotion for prepared statements to avoid a full binding.
     * @param parameters to be checked and possibly promoted
     * @param desired_parameter_types desired parameter types from the initial binding
     */
    static void PromoteParameters(common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters,
                                  const std::vector<execution::sql::SqlTypeId> &desired_parameter_types);

    /**
     * Attempt to convert the transient value to the desired type.
     * Note that type promotion could be an upcast or downcast size-wise.
     *
     * @param value The transient value to be checked and potentially promoted.
     * @param desired_type The type to promote the transient value to.
     */
    static void CheckAndTryPromoteType(common::ManagedPointer<parser::ConstantValueExpression> value,
                                       execution::sql::SqlTypeId                               desired_type);

    /**
     * Predicate for evaluating expressions that serve as WHERE clauses. This is mostly defined by postgres and what we
     * support, and should evolve as stuff gets fixed (mostly related to literals, at the moment).
     * @param value expression that serves as the condition in a WHERE clause (SELECT, UPDATE, INSERT)
     */
    static void ValidateWhereClause(common::ManagedPointer<parser::AbstractExpression> value);

    /**
     * @return True if the value of @p int_val fits in the Output type, false otherwise.
     */
    template <typename Output, typename Input>
    static auto IsRepresentable(Input int_val) -> bool;

    /**
     * @return Casted numeric type, or an exception if the cast fails.
     */
    template <typename Input>
    static void TryCastNumericAll(common::ManagedPointer<parser::ConstantValueExpression> value,
                                  Input                                                   int_val,
                                  execution::sql::SqlTypeId                               desired_type);
};

/// @cond DOXYGEN_IGNORE
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int8_t                    int_val,
                                                   const execution::sql::SqlTypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int16_t                   int_val,
                                                   const execution::sql::SqlTypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int32_t                   int_val,
                                                   const execution::sql::SqlTypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int64_t                   int_val,
                                                   const execution::sql::SqlTypeId desired_type);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int8_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int16_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int32_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int64_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<double>(const double int_val);
/// @endcond

} // namespace noisepage::binder
