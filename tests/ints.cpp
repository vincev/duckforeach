// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <limits>
#include <unordered_set>

namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Test int8, uint8, char")
{
    using SInt = int8_t;
    using UInt = uint8_t;
    constexpr UInt MAX_UVAL = std::numeric_limits<UInt>::max();

    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UTINYINT, sval TINYINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    ddb::Appender appender{con, "t"};
    for (UInt i{MAX_UVAL - 100}; i < MAX_UVAL; ++i)
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
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](UInt uval, SInt sval)
                                    {
                                        CHECK(uints.contains(uval));
                                        CHECK(sints.contains(sval));
                                        ++num_rows;
                                    }));

        CHECK_EQ(num_rows, sints.size());
    }

    SUBCASE("conversion to char")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](char uval, char sval)
                                    {
                                        CHECK(uints.contains(static_cast<UInt>(uval)));
                                        ++num_rows;
                                    }));

        CHECK_EQ(num_rows, sints.size());
    }

    SUBCASE("conversion to bigger integers")
    {
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](uint16_t uval, int16_t sval)
                                    {
                                        CHECK(uints.contains(static_cast<UInt>(uval)));
                                        CHECK(sints.contains(static_cast<SInt>(sval)));
                                    }));

        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](uint32_t uval, int32_t sval)
                                    {
                                        CHECK(uints.contains(static_cast<UInt>(uval)));
                                        CHECK(sints.contains(static_cast<SInt>(sval)));
                                    }));
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as uval column is unsigned.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](SInt uval, SInt sval) {}));

        // This should throw as sval column is signed.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw if number of columns different than number of arguments")
    {
        // These should throw as the query has two columns
        CHECK_THROWS(dfe::for_each(con.Query("select uval, sval from t"), [&](UInt sval) {}));
        CHECK_THROWS(dfe::for_each(con.Query("select uval, sval from t"),
                                   [&](UInt uval, SInt sval, SInt aval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, SInt sval) {}));

        size_t num_nulls{0}, num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](std::optional<UInt> uval, std::optional<SInt> sval)
                                    {
                                        if (!uval.has_value() || !sval.has_value())
                                            ++num_nulls;
                                        else
                                        {
                                            CHECK(uints.contains(static_cast<UInt>(*uval)));
                                            CHECK(sints.contains(static_cast<SInt>(*sval)));
                                            ++num_rows;
                                        }
                                    }));

        CHECK_EQ(num_nulls, 2);
        CHECK_EQ(num_rows, sints.size());
    }
}

TEST_CASE("Test int16, uint16")
{
    using SInt = int16_t;
    using UInt = uint16_t;
    constexpr UInt MAX_UVAL = std::numeric_limits<UInt>::max();

    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval USMALLINT, sval SMALLINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    ddb::Appender appender{con, "t"};
    for (UInt i{MAX_UVAL - 100}; i < MAX_UVAL; ++i)
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
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](UInt uval, SInt sval)
                                    {
                                        CHECK(uints.contains(uval));
                                        CHECK(sints.contains(sval));
                                        ++num_rows;
                                    }));

        CHECK_EQ(num_rows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        CHECK_THROWS(dfe::for_each(con.Query("select uval, sval from t"),
                                   [&](uint8_t uval, int8_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, SInt sval) {}));

        size_t num_nulls{0}, num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](std::optional<UInt> uval, std::optional<SInt> sval)
                                    {
                                        if (!uval.has_value() || !sval.has_value())
                                            ++num_nulls;
                                        else
                                        {
                                            CHECK(uints.contains(static_cast<UInt>(*uval)));
                                            CHECK(sints.contains(static_cast<SInt>(*sval)));
                                            ++num_rows;
                                        }
                                    }));

        CHECK_EQ(num_nulls, 2);
        CHECK_EQ(num_rows, sints.size());
    }
}

TEST_CASE("Test int32, uint32")
{
    using SInt = int32_t;
    using UInt = uint32_t;
    constexpr UInt MAX_UVAL = std::numeric_limits<UInt>::max();

    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UINTEGER, sval INTEGER)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    ddb::Appender appender{con, "t"};
    for (UInt i{MAX_UVAL - 100}; i < MAX_UVAL; ++i)
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
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](UInt uval, SInt sval)
                                    {
                                        CHECK(uints.contains(uval));
                                        CHECK(sints.contains(sval));
                                        ++num_rows;
                                    }));

        CHECK_EQ(num_rows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        CHECK_THROWS(dfe::for_each(con.Query("select uval, sval from t"),
                                   [&](uint16_t uval, int16_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, SInt sval) {}));

        size_t num_nulls{0}, num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](std::optional<UInt> uval, std::optional<SInt> sval)
                                    {
                                        if (!uval.has_value() || !sval.has_value())
                                            ++num_nulls;
                                        else
                                        {
                                            CHECK(uints.contains(static_cast<UInt>(*uval)));
                                            CHECK(sints.contains(static_cast<SInt>(*sval)));
                                            ++num_rows;
                                        }
                                    }));

        CHECK_EQ(num_nulls, 2);
        CHECK_EQ(num_rows, sints.size());
    }
}

TEST_CASE("Test int64, uint64")
{
    using SInt = int64_t;
    using UInt = uint64_t;
    constexpr UInt MAX_UVAL = std::numeric_limits<UInt>::max();

    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (uval UBIGINT, sval BIGINT)")};
    REQUIRE_FALSE(res->HasError());

    std::unordered_set<SInt> sints;
    std::unordered_set<UInt> uints;

    ddb::Appender appender{con, "t"};
    for (UInt i{MAX_UVAL - 100}; i < MAX_UVAL; ++i)
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
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](UInt uval, SInt sval)
                                    {
                                        CHECK(uints.contains(uval));
                                        CHECK(sints.contains(sval));
                                        ++num_rows;
                                    }));

        CHECK_EQ(num_rows, sints.size());
    }

    SUBCASE("throw if invalid signed conversion")
    {
        // This should throw as sval column is signed.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, UInt sval) {}));
    }

    SUBCASE("throw on conversion to smaller int overflow")
    {
        CHECK_THROWS(dfe::for_each(con.Query("select uval, sval from t"),
                                   [&](uint32_t uval, int32_t sval) {}));
    }

    SUBCASE("throw on conversion to char overflow")
    {
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](char uval, char sval) {}));
    }

    SUBCASE("handle nulls using optional parameters")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null,20), (10,null);")->HasError());

        // This should throw as plain types cannot handle nulls.
        CHECK_THROWS(
            dfe::for_each(con.Query("select uval, sval from t"), [&](UInt uval, SInt sval) {}));

        size_t num_nulls{0}, num_rows{0};
        CHECK_NOTHROW(dfe::for_each(con.Query("select uval, sval from t"),
                                    [&](std::optional<UInt> uval, std::optional<SInt> sval)
                                    {
                                        if (!uval.has_value() || !sval.has_value())
                                            ++num_nulls;
                                        else
                                        {
                                            CHECK(uints.contains(static_cast<UInt>(*uval)));
                                            CHECK(sints.contains(static_cast<SInt>(*sval)));
                                            ++num_rows;
                                        }
                                    }));

        CHECK_EQ(num_nulls, 2);
        CHECK_EQ(num_rows, sints.size());
    }
}

TEST_CASE("bool")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t (ival INTEGER)")};
    REQUIRE_FALSE(res->HasError());

    ddb::Appender appender{con, "t"};
    appender.AppendRow(-1);
    appender.AppendRow(0);
    appender.AppendRow(1);
    appender.Close();

    size_t num_true{0}, num_false{0};
    CHECK_NOTHROW(dfe::for_each(con.Query("select ival from t"),
                                [&](bool bval)
                                {
                                    if (bval)
                                        ++num_true;
                                    else
                                        ++num_false;
                                }));

    CHECK_EQ(num_false, 1);
    CHECK_EQ(num_true + num_false, 3);
}
