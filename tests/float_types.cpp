// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <unordered_set>

namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Test floating types")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (dval DOUBLE, fval FLOAT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<double> doubles;
    std::unordered_set<float> floats;

    ddb::Appender appender{con, "t"};
    for (int i{-500}; i < 500; ++i)
    {
        auto dval{static_cast<double>(i)};
        doubles.insert(dval);

        auto fval{static_cast<float>(i)};
        floats.insert(fval);

        appender.AppendRow(dval, fval);
    }
    appender.Close();

    SUBCASE("floats and doubles scan")
    {
        dfe::DuckForEach dfe{con.Query("select dval, fval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](double dval, float fval) {
            CHECK(doubles.contains(dval));
            CHECK(floats.contains(fval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, floats.size());
    }

    SUBCASE("conversion to integers")
    {
        dfe::DuckForEach dfe1{con.Query("select dval, fval from t")};
        CHECK_NOTHROW(dfe1([&](int16_t uval, int16_t sval) {
            CHECK(doubles.contains(static_cast<double>(uval)));
            CHECK(floats.contains(static_cast<float>(sval)));
        }));

        // Casting to unsigned should throw as there are negative values.
        dfe::DuckForEach dfe32{con.Query("select dval, fval from t")};
        CHECK_THROWS(dfe32([&](uint32_t uval, uint32_t sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20.0), (10.0,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        dfe::DuckForEach dfNoOpt{con.Query("select dval, fval from t")};
        CHECK_THROWS(dfNoOpt([&](double uval, float sval) {}));

        size_t numNulls{0};
        size_t numRows{0};

        dfe::DuckForEach dfe{con.Query("select dval, fval from t")};
        CHECK_NOTHROW(dfe([&](std::optional<double> dval, std::optional<float> fval) {
            if (!dval.has_value() || !fval.has_value())
                ++numNulls;
            else
            {
                CHECK(doubles.contains(*dval));
                CHECK(floats.contains(*fval));
                ++numRows;
            }
        }));

        CHECK_EQ(numNulls, 2);
        CHECK_EQ(numRows, floats.size());
    }
}
