// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <chrono>

namespace chr = std::chrono;
namespace ddb = duckdb;
namespace dfe = duckforeach;

namespace {

size_t gTestNumRows;

void testFunction(std::string sval, int32_t ival, dfe::Timestamp ts)
{
    gTestNumRows = ival;
}

struct TestFunctionObject
{
    std::string lastSval;
    int32_t lastIval;
    dfe::Timestamp lastTs;

    void operator()(std::string sval, int32_t ival, dfe::Timestamp ts)
    {
        lastSval = sval;
        lastIval = ival;
        lastTs = ts;
    }
};

} // namespace

TEST_CASE("Test iterating function types")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t ("
                       "  sval VARCHAR, "
                       "  ival INTEGER, "
                       "  tsval TIMESTAMP)")};
    REQUIRE_FALSE(res->HasError());

    const size_t numRows{10};
    for (size_t i{0}; i < numRows; ++i)
    {
        auto stm{std::format("INSERT INTO t VALUES ("
                             "'label{0}', "
                             "{0}, "
                             "'2024-06-{0:02} 11:30:{0:02}')",
                             i + 1)};
        REQUIRE_FALSE(con.Query(stm)->HasError());
    }

    SUBCASE("iterate with lambda")
    {
        dfe::DuckForEach dfe{con.Query("select sval, ival, tsval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string sval, int32_t ival, dfe::Timestamp ts) { ++rowCount; }));
        CHECK_EQ(rowCount, numRows);
    }

    SUBCASE("iterate with function pointer")
    {
        dfe::DuckForEach dfe{con.Query("select sval, ival, tsval from t")};
        CHECK_NOTHROW(dfe(testFunction));
        CHECK_EQ(gTestNumRows, numRows);
    }

    SUBCASE("iterate with function object")
    {
        dfe::DuckForEach dfe{con.Query("select sval, ival, tsval from t")};

        auto tfo{dfe(TestFunctionObject{})};

        CHECK_EQ(tfo.lastIval, numRows);
        CHECK_EQ(tfo.lastSval, std::format("label{}", numRows));
        CHECK_EQ(tfo.lastTs.ymd().day(), chr::day{numRows});
    }
}
