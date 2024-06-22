// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <format>
#include <iostream>
#include <limits>
#include <unordered_set>

using namespace duckdb;

TEST_CASE("Test int8, uint8, char")
{
    using SInt = int8_t;
    using UInt = uint8_t;
    constexpr UInt MaxUVal = std::numeric_limits<UInt>::max();

    DuckDB db;
    Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UTINYINT, sval TINYINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    Appender appender{con, "t"};
    for (UInt i{MaxUVal - 100}; i < MaxUVal; ++i)
    {
        auto sval{static_cast<SInt>(i)};
        sints.insert(sval);

        auto uval{static_cast<UInt>(i)};
        uints.insert(uval);

        appender.AppendRow(uval, sval);
    }
    appender.Close();

    SUBCASE("signed and unsigned TINYINT")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(f([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("conversion to char")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(f([&](char uval, char sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("conversion to bigger integers")
    {
        duckforeach::DuckForEach f16{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(f16([&](uint16_t uval, int16_t sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            CHECK(sints.contains(static_cast<SInt>(sval)));
        }));

        duckforeach::DuckForEach f32{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(f32([&](uint32_t uval, int32_t sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            CHECK(sints.contains(static_cast<SInt>(sval)));
        }));
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as uval column is unsigned.
        duckforeach::DuckForEach f1{con.Query("select uval, sval from t")};
        CHECK_THROWS(f1([&](SInt uval, SInt sval) {}));

        // This should throw as sval column is signed.
        duckforeach::DuckForEach f2{con.Query("select uval, sval from t")};
        CHECK_THROWS(f2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw if number of columns different than number of arguments")
    {
        // These should throw as the query has two columns
        duckforeach::DuckForEach f1{con.Query("select uval, sval from t")};
        CHECK_THROWS(f1([&](UInt sval) {}));

        duckforeach::DuckForEach f2{con.Query("select uval, sval from t")};
        CHECK_THROWS(f2([&](UInt uval, SInt sval, SInt aval) {}));
    }
}

TEST_CASE("Test int16, uint16")
{
    using SInt = int16_t;
    using UInt = uint16_t;
    constexpr UInt MaxUVal = std::numeric_limits<UInt>::max();

    DuckDB db;
    Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval USMALLINT, sval SMALLINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    Appender appender{con, "t"};
    for (UInt i{MaxUVal - 100}; i < MaxUVal; ++i)
    {
        auto sval{static_cast<SInt>(i)};
        sints.insert(sval);

        auto uval{static_cast<UInt>(i)};
        uints.insert(uval);

        appender.AppendRow(uval, sval);
    }
    appender.Close();

    SUBCASE("signed and unsigned SMALLINT")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(f([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        duckforeach::DuckForEach f2{con.Query("select uval, sval from t")};
        CHECK_THROWS(f2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](uint8_t uval, int8_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](char uval, char sval) {}));
    }
}

TEST_CASE("Test int32, uint32")
{
    using SInt = int32_t;
    using UInt = uint32_t;
    constexpr UInt MaxUVal = std::numeric_limits<UInt>::max();

    DuckDB db;
    Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UINTEGER, sval INTEGER)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    Appender appender{con, "t"};
    for (UInt i{MaxUVal - 100}; i < MaxUVal; ++i)
    {
        auto sval{static_cast<SInt>(i)};
        sints.insert(sval);

        auto uval{static_cast<UInt>(i)};
        uints.insert(uval);

        appender.AppendRow(uval, sval);
    }
    appender.Close();

    SUBCASE("signed and unsigned INTEGER")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(f([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        duckforeach::DuckForEach f2{con.Query("select uval, sval from t")};
        CHECK_THROWS(f2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](uint16_t uval, int16_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](char uval, char sval) {}));
    }
}

TEST_CASE("Test int64, uint64")
{
    using SInt = int64_t;
    using UInt = uint64_t;
    constexpr UInt MaxUVal = std::numeric_limits<UInt>::max();

    DuckDB db;
    Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UBIGINT, sval BIGINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    Appender appender{con, "t"};
    for (UInt i{MaxUVal - 100}; i < MaxUVal; ++i)
    {
        auto sval{static_cast<SInt>(i)};
        sints.insert(sval);

        auto uval{static_cast<UInt>(i)};
        uints.insert(uval);

        appender.AppendRow(uval, sval);
    }
    appender.Close();

    SUBCASE("signed and unsigned BIGINT")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(f([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        duckforeach::DuckForEach f2{con.Query("select uval, sval from t")};
        CHECK_THROWS(f2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](uint32_t uval, int32_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        duckforeach::DuckForEach f{con.Query("select uval, sval from t")};
        CHECK_THROWS(f([&](char uval, char sval) {}));
    }
}