#include "execution/sql/data_types.h"

#include <llvm/ADT/DenseMap.h>

#include <memory>
#include <string>
#include <utility>

namespace noisepage::execution::sql {

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

BooleanType::BooleanType(bool nullable)
    : SqlType(SqlTypeId::Boolean, nullable) {}

auto BooleanType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::Boolean;
}

auto BooleanType::GetName() const -> std::string {
    std::string str = "Boolean";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto BooleanType::IsIntegral() const -> bool {
    return false;
}

auto BooleanType::IsFloatingPoint() const -> bool {
    return false;
}

auto BooleanType::IsArithmetic() const -> bool {
    return false;
}

auto BooleanType::Equals(const SqlType &that) const -> bool {
    return that.Is<BooleanType>() && IsNullable() == that.IsNullable();
}

auto BooleanType::InstanceNonNullable() -> const BooleanType & {
    static BooleanType k_non_nullable_boolean(false);
    return k_non_nullable_boolean;
}

auto BooleanType::InstanceNullable() -> const BooleanType & {
    static BooleanType k_nullable_boolean(true);
    return k_nullable_boolean;
}

// ---------------------------------------------------------
// Tiny Integer
// ---------------------------------------------------------

TinyIntType::TinyIntType(bool nullable)
    : NumberBaseType(SqlTypeId::TinyInt, nullable) {}

auto TinyIntType::GetName() const -> std::string {
    std::string str = "TinyInt";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto TinyIntType::Equals(const SqlType &that) const -> bool {
    return that.Is<TinyIntType>() && IsNullable() == that.IsNullable();
}

auto TinyIntType::InstanceNonNullable() -> const TinyIntType & {
    static TinyIntType k_non_nullable_tiny_int(false);
    return k_non_nullable_tiny_int;
}

auto TinyIntType::InstanceNullable() -> const TinyIntType & {
    static TinyIntType k_nullable_tiny_int(true);
    return k_nullable_tiny_int;
}

// ---------------------------------------------------------
// Small Integer
// ---------------------------------------------------------

SmallIntType::SmallIntType(bool nullable)
    : NumberBaseType(SqlTypeId::SmallInt, nullable) {}

auto SmallIntType::GetName() const -> std::string {
    std::string str = "SmallInt";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto SmallIntType::Equals(const SqlType &that) const -> bool {
    return that.Is<SmallIntType>() && IsNullable() == that.IsNullable();
}

auto SmallIntType::InstanceNonNullable() -> const SmallIntType & {
    static SmallIntType k_non_nullable_small_int(false);
    return k_non_nullable_small_int;
}

auto SmallIntType::InstanceNullable() -> const SmallIntType & {
    static SmallIntType k_nullable_small_int(true);
    return k_nullable_small_int;
}

// ---------------------------------------------------------
// Integer
// ---------------------------------------------------------

IntegerType::IntegerType(bool nullable)
    : NumberBaseType(SqlTypeId::Integer, nullable) {}

auto IntegerType::GetName() const -> std::string {
    std::string str = "Integer";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto IntegerType::Equals(const SqlType &that) const -> bool {
    return that.Is<IntegerType>() && IsNullable() == that.IsNullable();
}

auto IntegerType::InstanceNonNullable() -> const IntegerType & {
    static IntegerType k_non_nullable_int(false);
    return k_non_nullable_int;
}

auto IntegerType::InstanceNullable() -> const IntegerType & {
    static IntegerType k_nullable_int(true);
    return k_nullable_int;
}

// ---------------------------------------------------------
// Big Integer
// ---------------------------------------------------------

BigIntType::BigIntType(bool nullable)
    : NumberBaseType(SqlTypeId::BigInt, nullable) {}

auto BigIntType::GetName() const -> std::string {
    std::string str = "BigInt";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto BigIntType::Equals(const SqlType &that) const -> bool {
    return that.Is<BigIntType>() && IsNullable() == that.IsNullable();
}

auto BigIntType::InstanceNonNullable() -> const BigIntType & {
    static BigIntType k_non_nullable_big_int(false);
    return k_non_nullable_big_int;
}

auto BigIntType::InstanceNullable() -> const BigIntType & {
    static BigIntType k_nullable_big_int(true);
    return k_nullable_big_int;
}

// ---------------------------------------------------------
// Real
// ---------------------------------------------------------

RealType::RealType(bool nullable)
    : NumberBaseType(SqlTypeId::Real, nullable) {}

auto RealType::GetName() const -> std::string {
    std::string str = "Real";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto RealType::Equals(const SqlType &that) const -> bool {
    return that.Is<RealType>() && IsNullable() == that.IsNullable();
}

auto RealType::InstanceNonNullable() -> const RealType & {
    static RealType k_non_nullable_real(false);
    return k_non_nullable_real;
}

auto RealType::InstanceNullable() -> const RealType & {
    static RealType k_nullable_real(true);
    return k_nullable_real;
}

// ---------------------------------------------------------
// Double
// ---------------------------------------------------------

DoubleType::DoubleType(bool nullable)
    : NumberBaseType(SqlTypeId::Double, nullable) {}

auto DoubleType::GetName() const -> std::string {
    std::string str = "Double";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto DoubleType::Equals(const SqlType &that) const -> bool {
    return that.Is<DoubleType>() && IsNullable() == that.IsNullable();
}

auto DoubleType::InstanceNonNullable() -> const DoubleType & {
    static DoubleType k_non_nullable_double(false);
    return k_non_nullable_double;
}

auto DoubleType::InstanceNullable() -> const DoubleType & {
    static DoubleType k_nullable_double(true);
    return k_nullable_double;
}

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

DecimalType::DecimalType(bool nullable, uint32_t precision, uint32_t scale)
    : SqlType(SqlTypeId::Decimal, nullable)
    , precision_(precision)
    , scale_(scale) {}

auto DecimalType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::BigInt;
}

auto DecimalType::GetName() const -> std::string {
    std::string str = "Decimal[" + std::to_string(Precision()) + "," + std::to_string(Scale());
    if (IsNullable()) {
        str.append(",NULLABLE");
    }
    str.append("]");
    return str;
}

auto DecimalType::Equals(const SqlType &that) const -> bool {
    if (auto *other_decimal = that.SafeAs<DecimalType>()) {
        return Precision() == other_decimal->Precision() && Scale() == other_decimal->Scale()
               && IsNullable() == that.IsNullable();
    }
    return false;
}

auto DecimalType::IsIntegral() const -> bool {
    return false;
}

auto DecimalType::IsFloatingPoint() const -> bool {
    return true;
}

auto DecimalType::IsArithmetic() const -> bool {
    return true;
}

auto DecimalType::Precision() const -> uint32_t {
    return precision_;
}

auto DecimalType::Scale() const -> uint32_t {
    return scale_;
}

template <bool Nullable>
auto DecimalType::InstanceInternal(uint32_t precision, uint32_t scale) -> const DecimalType & {
    static llvm::DenseMap<std::pair<uint32_t, uint32_t>, std::unique_ptr<DecimalType>> k_decimal_type_map;

    auto key = std::make_pair(precision, scale);
    if (auto iter = k_decimal_type_map.find(key); iter != k_decimal_type_map.end()) {
        return *iter->second;
    }

    auto iter = k_decimal_type_map.try_emplace(key, new DecimalType(Nullable, precision, scale));
    return *iter.first->second;
}

auto DecimalType::InstanceNonNullable(uint32_t precision, uint32_t scale) -> const DecimalType & {
    return InstanceInternal<false>(precision, scale);
}

auto DecimalType::InstanceNullable(uint32_t precision, uint32_t scale) -> const DecimalType & {
    return InstanceInternal<true>(precision, scale);
}

// ---------------------------------------------------------
// Date
// ---------------------------------------------------------

auto DateType::InstanceNonNullable() -> const DateType & {
    static DateType k_non_nullable_date(false);
    return k_non_nullable_date;
}

auto DateType::InstanceNullable() -> const DateType & {
    static DateType k_nullable_date(true);
    return k_nullable_date;
}

auto DateType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::Date;
}

auto DateType::GetName() const -> std::string {
    std::string str = "Date";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto DateType::Equals(const SqlType &that) const -> bool {
    return that.Is<DateType>() && IsNullable() == that.IsNullable();
}

DateType::DateType(bool nullable)
    : SqlType(SqlTypeId::Date, nullable) {}

// ---------------------------------------------------------
// Timestamp
// ---------------------------------------------------------

auto TimestampType::InstanceNonNullable() -> const TimestampType & {
    static TimestampType k_non_nullable_timestamp(false);
    return k_non_nullable_timestamp;
}

auto TimestampType::InstanceNullable() -> const TimestampType & {
    static TimestampType k_nullable_timestamp(true);
    return k_nullable_timestamp;
}

auto TimestampType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::Timestamp;
}

auto TimestampType::GetName() const -> std::string {
    std::string str = "Timestamp";
    if (IsNullable()) {
        str.append("[NULLABLE]");
    }
    return str;
}

auto TimestampType::Equals(const SqlType &that) const -> bool {
    return that.Is<TimestampType>() && IsNullable() && that.IsNullable();
}

TimestampType::TimestampType(bool nullable)
    : SqlType(SqlTypeId::Timestamp, nullable) {}

// ---------------------------------------------------------
// Fixed-length strings
// ---------------------------------------------------------

template <bool Nullable>
auto CharType::InstanceInternal(uint32_t length) -> const CharType & {
    static llvm::DenseMap<uint32_t, std::unique_ptr<CharType>> k_char_type_map;

    if (auto iter = k_char_type_map.find(length); iter != k_char_type_map.end()) {
        return *iter->second;
    }

    auto iter = k_char_type_map.try_emplace(length, new CharType(Nullable, length));
    return *iter.first->second;
}

auto CharType::InstanceNonNullable(uint32_t len) -> const CharType & {
    return InstanceInternal<false>(len);
}
auto CharType::InstanceNullable(uint32_t len) -> const CharType & {
    return InstanceInternal<true>(len);
}

CharType::CharType(bool nullable, uint32_t length)
    : SqlType(SqlTypeId::Char, nullable)
    , length_(length) {}

auto CharType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::Varchar;
}

auto CharType::GetName() const -> std::string {
    std::string str = "Char[" + std::to_string(Length());
    if (IsNullable()) {
        str.append(",NULLABLE");
    }
    str.append("]");
    return str;
}

auto CharType::Equals(const SqlType &that) const -> bool {
    if (auto *other_char = that.SafeAs<CharType>()) {
        return Length() == other_char->Length() && IsNullable() == other_char->IsNullable();
    }
    return false;
}

auto CharType::Length() const -> uint32_t {
    return length_;
}

// ---------------------------------------------------------
// Variable-length strings
// ---------------------------------------------------------

template <bool Nullable>
auto VarcharType::InstanceInternal(uint32_t length) -> const VarcharType & {
    static llvm::DenseMap<uint32_t, std::unique_ptr<VarcharType>> k_varchar_type_map;

    if (auto iter = k_varchar_type_map.find(length); iter != k_varchar_type_map.end()) {
        return *iter->second;
    }

    auto iter = k_varchar_type_map.try_emplace(length, new VarcharType(Nullable, length));
    return *iter.first->second;
}

auto VarcharType::InstanceNonNullable(uint32_t max_len) -> const VarcharType & {
    return InstanceInternal<false>(max_len);
}

auto VarcharType::InstanceNullable(uint32_t max_len) -> const VarcharType & {
    return InstanceInternal<true>(max_len);
}

VarcharType::VarcharType(bool nullable, uint32_t max_len)
    : SqlType(SqlTypeId::Varchar, nullable)
    , max_len_(max_len) {}

auto VarcharType::GetPrimitiveTypeId() const -> TypeId {
    return TypeId::Varchar;
}

auto VarcharType::GetName() const -> std::string {
    std::string str = "Varchar[" + std::to_string(MaxLength());
    if (IsNullable()) {
        str.append(",NULLABLE");
    }
    str.append("]");
    return str;
}

auto VarcharType::Equals(const SqlType &that) const -> bool {
    if (auto *other_varchar = that.SafeAs<VarcharType>()) {
        return MaxLength() == other_varchar->MaxLength() && IsNullable() == other_varchar->IsNullable();
    }
    return false;
}

auto VarcharType::MaxLength() const -> uint32_t {
    return max_len_;
}

} // namespace noisepage::execution::sql
