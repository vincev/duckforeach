// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <iostream>
#include <unordered_set>

namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Test strings")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t ("
                       "  strval VARCHAR, "
                       "  ival INTEGER, "
                       "  rval REAL, "
                       "  dtval DATE, "
                       "  tmval TIME, "
                       "  tsval TIMESTAMP, "
                       "  intval INTERVAL)")};
    REQUIRE_FALSE(res->HasError());

    for (size_t i{0}; i < 10; ++i)
    {
        auto stm{std::format("INSERT INTO t VALUES ("
                             "'label{0}', "
                             "{0}, "
                             "{0}, "
                             "'2024-06-{0:02}', "
                             "'11:30:{0:02}',"
                             "'2024-06-{0:02} 11:30:{0:02}', "
                             "'{0} hours')",
                             i + 1)};
        REQUIRE_FALSE(con.Query(stm)->HasError());
    }

    SUBCASE("string by value")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select strval from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("label{}", num_rows));
                                    }));
    }

    SUBCASE("string by rvalue reference")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select strval from t"),
                                    [&](std::string&& s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("label{}", num_rows));
                                    }));
    }

    SUBCASE("string by const reference")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select strval from t"),
                                    [&](const std::string& s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("label{}", num_rows));
                                    }));
    }

    SUBCASE("integer to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select ival from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("{}", num_rows));
                                    }));
    }

    SUBCASE("float to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select rval from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("{}.0", num_rows));
                                    }));
    }

    SUBCASE("date to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select dtval from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("2024-06-{:02}", num_rows));
                                    }));
    }

    SUBCASE("time to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select tmval from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("11:30:{:02}", num_rows));
                                    }));
    }

    SUBCASE("timestamp to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(
            dfe::for_each(con.Query("select tsval from t"),
                          [&](std::string s)
                          {
                              ++num_rows;
                              CHECK_EQ(s, std::format("2024-06-{0:02} 11:30:{0:02}", num_rows));
                          }));
    }

    SUBCASE("interval to string conversion")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select intval from t"),
                                    [&](std::string s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(s, std::format("{0:02}:00:00", num_rows));
                                    }));
    }
}

TEST_CASE("Test null strings")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (strval VARCHAR)")};
    REQUIRE_FALSE(res->HasError());

    constexpr size_t NUM_ROWS{10};

    for (size_t i{0}; i < NUM_ROWS; ++i)
    {
        auto stm{std::format("INSERT INTO t VALUES ('label{0}')", i + 1)};
        REQUIRE_FALSE(con.Query(stm)->HasError());
    }

    REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null)")->HasError());

    size_t num_rows{0}, num_nulls{0};
    CHECK_NOTHROW(dfe::for_each(con.Query("select strval from t"),
                                [&](std::optional<std::string> s)
                                {
                                    if (s)
                                    {
                                        ++num_rows;
                                        CHECK_EQ(*s, std::format("label{}", num_rows));
                                    }
                                    else
                                    {
                                        ++num_nulls;
                                    }
                                }));
    CHECK_EQ(num_rows, NUM_ROWS);
    CHECK_EQ(num_nulls, 1);
}