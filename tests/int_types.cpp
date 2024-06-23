// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <format>
#include <iostream>
#include <limits>
#include <unordered_set>

using namespace duckdb;
namespace dfe = duckforeach;

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
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("conversion to char")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](char uval, char sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("conversion to bigger integers")
    {
        dfe::DuckForEach dfe16{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe16([&](uint16_t uval, int16_t sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            CHECK(sints.contains(static_cast<SInt>(sval)));
        }));

        dfe::DuckForEach dfe32{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe32([&](uint32_t uval, int32_t sval) {
            CHECK(uints.contains(static_cast<UInt>(uval)));
            CHECK(sints.contains(static_cast<SInt>(sval)));
        }));
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as uval column is unsigned.
        dfe::DuckForEach dfe1{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe1([&](SInt uval, SInt sval) {}));

        // This should throw as sval column is signed.
        dfe::DuckForEach dfe2{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw if number of columns different than number of arguments")
    {
        // These should throw as the query has two columns
        dfe::DuckForEach dfe1{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe1([&](UInt sval) {}));

        dfe::DuckForEach dfe2{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe2([&](UInt uval, SInt sval, SInt aval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        size_t numNulls{0};
        size_t numRows{0};

        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe([&](std::optional<UInt> uval, std::optional<SInt> sval) {
            if (!uval.has_value() || !sval.has_value())
                ++numNulls;
            else
            {
                CHECK(uints.contains(static_cast<UInt>(*uval)));
                CHECK(sints.contains(static_cast<SInt>(*sval)));
                ++numRows;
            }
        }));

        CHECK_EQ(numNulls, 2);
        CHECK_EQ(numRows, sints.size());
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
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        dfe::DuckForEach dfe2{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](uint8_t uval, int8_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        size_t numNulls{0};
        size_t numRows{0};

        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe([&](std::optional<UInt> uval, std::optional<SInt> sval) {
            if (!uval.has_value() || !sval.has_value())
                ++numNulls;
            else
            {
                CHECK(uints.contains(static_cast<UInt>(*uval)));
                CHECK(sints.contains(static_cast<SInt>(*sval)));
                ++numRows;
            }
        }));

        CHECK_EQ(numNulls, 2);
        CHECK_EQ(numRows, sints.size());
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
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        dfe::DuckForEach dfe2{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](uint16_t uval, int16_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        size_t numNulls{0};
        size_t numRows{0};

        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe([&](std::optional<UInt> uval, std::optional<SInt> sval) {
            if (!uval.has_value() || !sval.has_value())
                ++numNulls;
            else
            {
                CHECK(uints.contains(static_cast<UInt>(*uval)));
                CHECK(sints.contains(static_cast<SInt>(*sval)));
                ++numRows;
            }
        }));

        CHECK_EQ(numNulls, 2);
        CHECK_EQ(numRows, sints.size());
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
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};

        size_t foundRows{0};
        CHECK_NOTHROW(dfe([&](UInt uval, SInt sval) {
            CHECK(uints.contains(uval));
            CHECK(sints.contains(sval));
            ++foundRows;
        }));

        CHECK_EQ(foundRows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        dfe::DuckForEach dfe2{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe2([&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](uint32_t uval, int32_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_THROWS(dfe([&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        size_t numNulls{0};
        size_t numRows{0};

        dfe::DuckForEach dfe{con.Query("select uval, sval from t")};
        CHECK_NOTHROW(dfe([&](std::optional<UInt> uval, std::optional<SInt> sval) {
            if (!uval.has_value() || !sval.has_value())
                ++numNulls;
            else
            {
                CHECK(uints.contains(static_cast<UInt>(*uval)));
                CHECK(sints.contains(static_cast<SInt>(*sval)));
                ++numRows;
            }
        }));

        CHECK_EQ(numNulls, 2);
        CHECK_EQ(numRows, sints.size());
    }
}