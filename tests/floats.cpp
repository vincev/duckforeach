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
        size_t found_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select dval, fval from t"),
                                    [&](double dval, float fval)
                                    {
                                        CHECK(doubles.contains(dval));
                                        CHECK(floats.contains(fval));
                                        ++found_rows;
                                    }));

        CHECK_EQ(found_rows, floats.size());
    }

    SUBCASE("conversion to integers")
    {
        CHECK_NOTHROW(dfe::for_each(con.Query("select dval, fval from t"),
                                    [&](int16_t uval, int16_t sval)
                                    {
                                        CHECK(doubles.contains(static_cast<double>(uval)));
                                        CHECK(floats.contains(static_cast<float>(sval)));
                                    }));

        // Casting to unsigned should throw as there are negative values.
        CHECK_THROWS(dfe::for_each(con.Query("select dval, fval from t"),
                                   [&](uint32_t uval, uint32_t sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20.0), (10.0,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        CHECK_THROWS(
            dfe::for_each(con.Query("select dval, fval from t"), [&](double uval, float sval) {}));

        size_t found_nulls{0}, found_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select dval, fval from t"),
                                    [&](std::optional<double> dval, std::optional<float> fval)
                                    {
                                        if (!dval.has_value() || !fval.has_value())
                                            ++found_nulls;
                                        else
                                        {
                                            CHECK(doubles.contains(*dval));
                                            CHECK(floats.contains(*fval));
                                            ++found_rows;
                                        }
                                    }));

        CHECK_EQ(found_nulls, 2);
        CHECK_EQ(found_rows, floats.size());
    }
}
