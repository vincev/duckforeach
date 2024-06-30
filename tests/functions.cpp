// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <chrono>

namespace chr = std::chrono;
namespace ddb = duckdb;
namespace dfe = duckforeach;

namespace {

size_t g_num_rows;

void test_function(std::string sval, int32_t ival, dfe::Timestamp ts)
{
    g_num_rows = ival;
}

struct TestFunctionObject
{
    std::string sval;
    int32_t ival;
    dfe::Timestamp tsval;

    void operator()(std::string s, int32_t i, dfe::Timestamp ts)
    {
        sval = s;
        ival = i;
        tsval = ts;
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

    constexpr size_t NUM_ROWS{10};
    for (size_t i{0}; i < NUM_ROWS; ++i)
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
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select sval, ival, tsval from t"),
                                    [&](std::string sval, int32_t ival, dfe::Timestamp ts)
                                    { ++num_rows; }));
        CHECK_EQ(num_rows, NUM_ROWS);
    }

    SUBCASE("iterate with function pointer")
    {
        CHECK_NOTHROW(dfe::for_each(con.Query("select sval, ival, tsval from t"), test_function));
        CHECK_EQ(g_num_rows, NUM_ROWS);
    }

    SUBCASE("iterate with function object")
    {
        auto tfo{dfe::for_each(con.Query("select sval, ival, tsval from t"), TestFunctionObject{})};
        CHECK_EQ(tfo.ival, NUM_ROWS);
        CHECK_EQ(tfo.sval, std::format("label{}", NUM_ROWS));
        CHECK_EQ(tfo.tsval.ymd().day(), chr::day{NUM_ROWS});
    }
}
