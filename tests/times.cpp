// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "doctest.h"

#include "duckforeach.hpp"

#include <format>
#include <iostream>
#include <limits>
#include <unordered_set>

namespace chr = std::chrono;
namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Test time types")
{
    ddb::DuckDB db;
    ddb::Connection con{db};

    auto res{con.Query("CREATE TABLE t ("
                       "  dtval DATE, "
                       "  tmval TIME, "
                       "  tsval TIMESTAMP, "
                       "  ival INTERVAL)")};
    REQUIRE_FALSE(res->HasError());

    constexpr size_t NUM_ROWS{10};
    for (size_t i{0}; i < NUM_ROWS; ++i)
    {
        auto stm{std::format("INSERT INTO t VALUES ("
                             "'2024-06-{0:02}', "
                             "'11:30:{0:02}',"
                             "'2024-06-{0:02} 11:30:{0:02}', "
                             "'{0} hours')",
                             i + 1)};
        REQUIRE_FALSE(con.Query(stm)->HasError());
    }

    SUBCASE("date, time, timestamp and interval")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(
            con.Query("select dtval, tmval, tsval, ival from t"),
            [&](ddb::date_t dtval, ddb::dtime_t tmval, ddb::timestamp_t tsval, ddb::interval_t ival)
            {
                ++num_rows;

                CHECK_EQ(ddb::Date::ExtractYear(dtval), 2024);
                CHECK_EQ(ddb::Date::ExtractMonth(dtval), 6);
                CHECK_EQ(ddb::Date::ExtractDay(dtval), num_rows);

                int32_t hour, mins, secs, micros;
                ddb::Time::Convert(tmval, hour, mins, secs, micros);
                CHECK_EQ(hour, 11);
                CHECK_EQ(mins, 30);
                CHECK_EQ(secs, num_rows);
                CHECK_EQ(micros, 0);

                ddb::date_t tsDate;
                ddb::dtime_t tsTime;
                ddb::Timestamp::Convert(tsval, tsDate, tsTime);
                CHECK_EQ(tsDate, dtval);
                CHECK_EQ(tsTime, tmval);

                CHECK_EQ(ddb::Interval::GetMilli(ival), num_rows * 3'600'000);
            }));

        CHECK_EQ(num_rows, NUM_ROWS);
    }

    SUBCASE("duckforeach year_month_day, hh_mm_ss, timestamp")
    {
        unsigned num_rows{0};
        CHECK_NOTHROW(dfe::for_each(
            con.Query("select dtval, tmval, tsval from t"),
            [&](dfe::year_month_day ymd, dfe::hh_mm_ss hms, dfe::Timestamp ts)
            {
                ++num_rows;

                dfe::year_month_day expectedYmd{chr::year{2024}, chr::month{6}, chr::day{num_rows}};
                CHECK_EQ(ymd, expectedYmd);

                CHECK_EQ(hms.hours(), chr::hours{11});
                CHECK_EQ(hms.minutes(), chr::minutes{30});
                CHECK_EQ(hms.seconds(), chr::seconds{num_rows});

                CHECK_EQ(ts.ymd(), ymd);
                CHECK_EQ(ts.hms().hours(), chr::hours{11});
                CHECK_EQ(ts.hms().minutes(), chr::minutes{30});
                CHECK_EQ(ts.hms().seconds(), chr::seconds{num_rows});
            }));
    }

    SUBCASE("string conversions")
    {
        size_t num_rows{0};
        CHECK_NOTHROW(dfe::for_each(
            con.Query("select dtval, tmval, tsval, ival from t"),
            [&](std::string dtval, std::string tmval, std::string tsval, std::string ival)
            {
                ++num_rows;
                CHECK_EQ(dtval, std::format("2024-06-{:02}", num_rows));
                CHECK_EQ(tmval, std::format("11:30:{:02}", num_rows));
                CHECK_EQ(tsval, std::format("2024-06-{0:02} 11:30:{0:02}", num_rows));
                CHECK_EQ(ival, std::format("{0:02}:00:00", num_rows));
            }));
    }

    SUBCASE("handle nulls")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null, null, null, null)")->HasError());

        SUBCASE("error on null date without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select dtval from t"), [&](ddb::date_t dtval) {}));
        }

        SUBCASE("handle null date with optional")
        {
            size_t num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select dtval from t"),
                                        [&](std::optional<ddb::date_t> dtval)
                                        {
                                            if (!dtval)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                CHECK_EQ(ddb::Date::ExtractYear(*dtval), 2024);
                                                CHECK_EQ(ddb::Date::ExtractMonth(*dtval), 6);
                                                CHECK_EQ(ddb::Date::ExtractDay(*dtval), num_rows);
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("error on null time without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select tmval from t"), [&](ddb::dtime_t tmval) {}));
        }

        SUBCASE("handle null time with optional")
        {
            size_t num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select tmval from t"),
                                        [&](std::optional<ddb::dtime_t> tmval)
                                        {
                                            if (!tmval)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                int32_t hour, mins, secs, micros;
                                                ddb::Time::Convert(*tmval, hour, mins, secs,
                                                                   micros);
                                                CHECK_EQ(hour, 11);
                                                CHECK_EQ(mins, 30);
                                                CHECK_EQ(secs, num_rows);
                                                CHECK_EQ(micros, 0);
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("error on null timestamp without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select tmval from t"), [&](ddb::timestamp_t tsval) {}));
        }

        SUBCASE("handle null timestamp with optional")
        {
            unsigned num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select tsval from t"),
                                        [&](std::optional<ddb::timestamp_t> tsval)
                                        {
                                            namespace chr = std::chrono;
                                            if (!tsval)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                ddb::date_t tsDate;
                                                ddb::dtime_t tsTime;
                                                ddb::Timestamp::Convert(*tsval, tsDate, tsTime);

                                                CHECK_EQ(ddb::Date::ExtractYear(tsDate), 2024);
                                                CHECK_EQ(ddb::Date::ExtractMonth(tsDate), 6);
                                                CHECK_EQ(ddb::Date::ExtractDay(tsDate), num_rows);

                                                int32_t hour, mins, secs, micros;
                                                ddb::Time::Convert(tsTime, hour, mins, secs,
                                                                   micros);
                                                CHECK_EQ(hour, 11);
                                                CHECK_EQ(mins, 30);
                                                CHECK_EQ(secs, num_rows);
                                                CHECK_EQ(micros, 0);
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("error on null interval without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select ival from t"), [&](ddb::interval_t dtval) {}));
        }

        SUBCASE("handle null interval with optional")
        {
            size_t num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select ival from t"),
                                        [&](std::optional<ddb::interval_t> ival)
                                        {
                                            if (!ival)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                CHECK_EQ(ddb::Interval::GetMilli(*ival),
                                                         num_rows * 3'600'000);
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("year_month_day without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select dtval from t"), [&](dfe::year_month_day ymd) {}));
        }

        SUBCASE("year_month_day with optional")
        {
            unsigned num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select dtval from t"),
                                        [&](std::optional<dfe::year_month_day> ymd)
                                        {
                                            if (!ymd)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                dfe::year_month_day expectedYmd{chr::year{2024},
                                                                                chr::month{6},
                                                                                chr::day{num_rows}};
                                                CHECK_EQ(*ymd, expectedYmd);
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("hh_mm_ss without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select tmval from t"), [&](dfe::hh_mm_ss hms) {}));
        }

        SUBCASE("hh_mm_ss with optional")
        {
            unsigned num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(con.Query("select tmval from t"),
                                        [&](std::optional<dfe::hh_mm_ss> hms)
                                        {
                                            if (!hms)
                                                ++num_nulls;
                                            else
                                            {
                                                ++num_rows;
                                                CHECK_EQ(hms->hours(), chr::hours{11});
                                                CHECK_EQ(hms->minutes(), chr::minutes{30});
                                                CHECK_EQ(hms->seconds(), chr::seconds{num_rows});
                                            }
                                        }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }

        SUBCASE("timestamp without optional")
        {
            CHECK_THROWS(
                dfe::for_each(con.Query("select tsval from t"), [&](dfe::Timestamp ts) {}));
        }

        SUBCASE("timestamp with optional")
        {
            unsigned num_nulls{0}, num_rows{0};
            CHECK_NOTHROW(dfe::for_each(
                con.Query("select tsval from t"),
                [&](std::optional<dfe::Timestamp> ts)
                {
                    if (!ts)
                        ++num_nulls;
                    else
                    {
                        ++num_rows;
                        dfe::year_month_day ymd{chr::year{2024}, chr::month{6}, chr::day{num_rows}};
                        CHECK_EQ(ts->ymd(), ymd);

                        dfe::hh_mm_ss hms{chr::hours{11} + chr::minutes{30} +
                                          chr::seconds{num_rows}};
                        CHECK_EQ(ts->hms().hours(), hms.hours());
                        CHECK_EQ(ts->hms().minutes(), hms.minutes());
                        CHECK_EQ(ts->hms().seconds(), hms.seconds());
                        CHECK_EQ(ts->hms().subseconds(), hms.subseconds());
                    }
                }));

            CHECK_EQ(num_nulls, 1);
            CHECK_EQ(num_rows, NUM_ROWS);
        }
    }
}

TEST_CASE("Test Timestamp comparisons")
{
    auto ts1{dfe::Timestamp::now()};
    auto ts2{dfe::Timestamp(ts1.time() + chr::minutes{1})};

    CHECK_EQ(ts1, ts1);
    CHECK(ts1 < ts2);
    CHECK(ts2 > ts1);
}

TEST_CASE("Test Timestamp hashing")
{
    std::unordered_set<dfe::Timestamp> set;

    auto ts1{dfe::Timestamp::now()};
    set.insert(ts1);

    for (int i{1}; i < 100; ++i)
        set.insert(dfe::Timestamp(ts1.time() + chr::seconds{i}));

    CHECK_EQ(set.size(), 100);

    for (int i{1}; i < 100; ++i)
        CHECK(set.contains(dfe::Timestamp(ts1.time() + chr::seconds{i})));
}

TEST_CASE("Test Timestamp formatting")
{
    dfe::year_month_day ymd{chr::year{2024} / 6 / 25};
    dfe::hh_mm_ss hms{chr::hours{11} + chr::minutes{30} + chr::seconds{25}};
    dfe::Timestamp ts{std::chrono::sys_days{ymd} + hms.to_duration() + chr::nanoseconds{123456789}};

    CHECK_EQ(std::format("{0:%Y}-{0:%m}-{0:%d}", ts), "2024-06-25");
    CHECK_EQ(std::format("{0:%H}:{0:%M}:{0:%S}", ts), "11:30:25.123456789");
    CHECK_EQ(std::format("{:%T}", ts), "11:30:25.123456789");
}