#include "execution/sql/generic_value.h"

#include <string>

#include "common/error/exception.h"
#include "common/math_util.h"
#include "execution/sql/constant_vector.h"
#include "execution/sql/value.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

auto GenericValue::Equals(const GenericValue &other) const -> bool {
    if (type_id_ != other.type_id_) {
        return false;
    }
    if (is_null_ != other.is_null_) {
        return false;
    }
    if (is_null_ && other.is_null_) {
        return true;
    }
    switch (type_id_) {
    case TypeId::Boolean:
        return value_.boolean_ == other.value_.boolean_;
    case TypeId::TinyInt:
        return value_.tinyint_ == other.value_.tinyint_;
    case TypeId::SmallInt:
        return value_.smallint_ == other.value_.smallint_;
    case TypeId::Integer:
        return value_.integer_ == other.value_.integer_;
    case TypeId::BigInt:
        return value_.bigint_ == other.value_.bigint_;
    case TypeId::Hash:
        return value_.hash_ == other.value_.hash_;
    case TypeId::Pointer:
        return value_.pointer_ == other.value_.pointer_;
    case TypeId::Float:
        return common::MathUtil::ApproxEqual(value_.float_, other.value_.float_);
    case TypeId::Double:
        return common::MathUtil::ApproxEqual(value_.double_, other.value_.double_);
    case TypeId::Date:
        return value_.date_ == other.value_.date_;
    case TypeId::Varchar:
        return str_value_ == other.str_value_;
    default:
        throw NOT_IMPLEMENTED_EXCEPTION("Equality of unsupported generic value.");
    }
    return false;
}

auto GenericValue::CastTo(const exec::ExecutionSettings &exec_settings, TypeId type) -> GenericValue {
    // Copy if same type
    if (type_id_ == type) {
        return GenericValue(*this);
    }
    // Use vector to cast
    ConstantVector result(*this);
    result.Cast(exec_settings, type);
    return result.GetValue(0);
}

auto GenericValue::ToString() const -> std::string {
    if (is_null_) {
        return "NULL";
    }
    switch (type_id_) {
    case TypeId::Boolean:
        return value_.boolean_ ? "True" : "False";
    case TypeId::TinyInt:
        return std::to_string(value_.tinyint_);
    case TypeId::SmallInt:
        return std::to_string(value_.smallint_);
    case TypeId::Integer:
        return std::to_string(value_.integer_);
    case TypeId::BigInt:
        return std::to_string(value_.bigint_);
    case TypeId::Hash:
        return std::to_string(value_.hash_);
    case TypeId::Pointer:
        return std::to_string(value_.pointer_);
    case TypeId::Float:
        return std::to_string(value_.float_);
    case TypeId::Double:
        return std::to_string(value_.double_);
    case TypeId::Date:
        return value_.date_.ToString();
    case TypeId::Timestamp:
        return value_.timestamp_.ToString();
    case TypeId::Varchar:
        return "'" + str_value_ + "'";
    default:
        throw NOT_IMPLEMENTED_EXCEPTION("String-ification of unsupported generic value.");
    }
}

auto operator<<(std::ostream &out, const GenericValue &val) -> std::ostream & {
    out << val.ToString();
    return out;
}

auto GenericValue::CreateNull(TypeId type_id) -> GenericValue {
    GenericValue result(type_id);
    result.is_null_ = true;
    return result;
}

auto GenericValue::CreateBoolean(const bool value) -> GenericValue {
    GenericValue result(TypeId::Boolean);
    result.value_.boolean_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateTinyInt(const int8_t value) -> GenericValue {
    GenericValue result(TypeId::TinyInt);
    result.value_.tinyint_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateSmallInt(const int16_t value) -> GenericValue {
    GenericValue result(TypeId::SmallInt);
    result.value_.smallint_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateInteger(const int32_t value) -> GenericValue {
    GenericValue result(TypeId::Integer);
    result.value_.integer_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateBigInt(const int64_t value) -> GenericValue {
    GenericValue result(TypeId::BigInt);
    result.value_.bigint_ = value;
    result.is_null_ = false;
    return result;
}
auto GenericValue::CreateHash(hash_t value) -> GenericValue {
    GenericValue result(TypeId::Hash);
    result.value_.hash_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreatePointer(uintptr_t value) -> GenericValue {
    GenericValue result(TypeId::Pointer);
    result.value_.pointer_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateReal(const float value) -> GenericValue {
    GenericValue result(TypeId::Float);
    result.value_.float_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateDouble(const double value) -> GenericValue {
    GenericValue result(TypeId::Double);
    result.value_.double_ = value;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateDate(Date date) -> GenericValue {
    GenericValue result(TypeId::Date);
    result.value_.date_ = date;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateDate(uint32_t year, uint32_t month, uint32_t day) -> GenericValue {
    return CreateDate(Date::FromYMD(year, month, day));
}

auto GenericValue::CreateTimestamp(Timestamp timestamp) -> GenericValue {
    GenericValue result(TypeId::Timestamp);
    result.value_.timestamp_ = timestamp;
    result.is_null_ = false;
    return result;
}

auto GenericValue::CreateTimestamp(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec)
    -> GenericValue {
    return CreateTimestamp(Timestamp::FromYMDHMS(year, month, day, hour, min, sec));
}

auto GenericValue::CreateVarchar(std::string_view str) -> GenericValue {
    GenericValue result(TypeId::Varchar);
    result.is_null_ = false;
    result.str_value_ = str;
    return result;
}

auto GenericValue::CreateFromRuntimeValue(const TypeId type_id, const Val &val) -> GenericValue {
    switch (type_id) {
    case TypeId::Boolean:
        return GenericValue::CreateBoolean(static_cast<const BoolVal &>(val).val_);
    case TypeId::TinyInt:
        return GenericValue::CreateTinyInt(static_cast<const Integer &>(val).val_);
    case TypeId::SmallInt:
        return GenericValue::CreateSmallInt(static_cast<const Integer &>(val).val_);
    case TypeId::Integer:
        return GenericValue::CreateInteger(static_cast<const Integer &>(val).val_);
    case TypeId::BigInt:
        return GenericValue::CreateBigInt(static_cast<const Integer &>(val).val_);
    case TypeId::Float:
        // TODO(tanujnay112): not sure if the data loss here would be an issue
        return GenericValue::CreateFloat(static_cast<float>(static_cast<const Real &>(val).val_));
    case TypeId::Double:
        return GenericValue::CreateDouble(static_cast<const Real &>(val).val_);
    case TypeId::Date:
        return GenericValue::CreateDate(static_cast<const DateVal &>(val).val_);
    case TypeId::Timestamp:
        return GenericValue::CreateTimestamp(static_cast<const TimestampVal &>(val).val_);
    case TypeId::Varchar:
        return GenericValue::CreateVarchar(static_cast<const StringVal &>(val).val_.StringView());
    default:
        throw NOT_IMPLEMENTED_EXCEPTION("Unsupported runtime value as generic value.");
    }
}

} // namespace noisepage::execution::sql
