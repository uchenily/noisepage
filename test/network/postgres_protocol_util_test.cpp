#include "network/postgres/postgres_protocol_util.h"

#include <vector>

#include "test_util/test_harness.h"
#include "gtest/gtest.h"

namespace noisepage::network {

class PostgresProtocolUtilTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(PostgresProtocolUtilTests, TypeConversionTest) {
    // Check that we can correctly convert our types back and forth from Postgres types

    // I hate C++ enums so much. Just give me a fucking iterator...
    std::vector<execution::sql::SqlTypeId> all_types = {
        execution::sql::SqlTypeId::Boolean,
        // TINYINT is aliased to boolean in Postgres, so we'll skip it
        // execution::sql::SqlTypeId::TinyInt,
        execution::sql::SqlTypeId::SmallInt,
        execution::sql::SqlTypeId::Integer,
        execution::sql::SqlTypeId::BigInt,
        execution::sql::SqlTypeId::Double,
        execution::sql::SqlTypeId::Timestamp,
        execution::sql::SqlTypeId::Date,
        execution::sql::SqlTypeId::Varchar,
        execution::sql::SqlTypeId::Varbinary,
    };
    for (execution::sql::SqlTypeId orig_internal_type : all_types) {
        auto postgres_type = PostgresProtocolUtil::InternalValueTypeToPostgresValueType(orig_internal_type);
        auto internal_type = PostgresProtocolUtil::PostgresValueTypeToInternalValueType(postgres_type);

        EXPECT_NE(PostgresValueType::INVALID, postgres_type);
        EXPECT_EQ(internal_type, orig_internal_type);
    }
}

} // namespace noisepage::network
