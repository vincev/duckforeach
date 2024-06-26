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

    const size_t numRows{10};
    for (size_t i{0}; i < numRows; ++i)
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
        dfe::DuckForEach dfe{con.Query("select strval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("label{}", rowCount));
        }));
    }

    SUBCASE("string by rvalue reference")
    {
        dfe::DuckForEach dfe{con.Query("select strval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string&& s) {
            ++rowCount;
            CHECK_EQ(s, std::format("label{}", rowCount));
        }));
    }

    SUBCASE("string by const reference")
    {
        dfe::DuckForEach dfe{con.Query("select strval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](const std::string& s) {
            ++rowCount;
            CHECK_EQ(s, std::format("label{}", rowCount));
        }));
    }

    SUBCASE("integer to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select ival from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("{}", rowCount));
        }));
    }

    SUBCASE("float to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select rval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("{}.0", rowCount));
        }));
    }

    SUBCASE("date to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select dtval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("2024-06-{:02}", rowCount));
        }));
    }

    SUBCASE("time to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select tmval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("11:30:{:02}", rowCount));
        }));
    }

    SUBCASE("timestamp to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select tsval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("2024-06-{0:02} 11:30:{0:02}", rowCount));
        }));
    }

    SUBCASE("interval to string conversion")
    {
        dfe::DuckForEach dfe{con.Query("select intval from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](std::string s) {
            ++rowCount;
            CHECK_EQ(s, std::format("{0:02}:00:00", rowCount));
        }));
    }
}
