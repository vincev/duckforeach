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

    const size_t numRows{10};
    for (size_t i{0}; i < numRows; ++i)
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
        dfe::DuckForEach dfe{con.Query("select dtval, tmval, tsval, ival from t")};

        size_t rowCount{0};
        CHECK_NOTHROW(dfe([&](ddb::date_t dtval, ddb::dtime_t tmval, ddb::timestamp_t tsval,
                              ddb::interval_t ival) {
            ++rowCount;

            CHECK_EQ(ddb::Date::ExtractYear(dtval), 2024);
            CHECK_EQ(ddb::Date::ExtractMonth(dtval), 6);
            CHECK_EQ(ddb::Date::ExtractDay(dtval), rowCount);

            int32_t hour, mins, secs, micros;
            ddb::Time::Convert(tmval, hour, mins, secs, micros);
            CHECK_EQ(hour, 11);
            CHECK_EQ(mins, 30);
            CHECK_EQ(secs, rowCount);
            CHECK_EQ(micros, 0);

            ddb::date_t tsDate;
            ddb::dtime_t tsTime;
            ddb::Timestamp::Convert(tsval, tsDate, tsTime);
            CHECK_EQ(tsDate, dtval);
            CHECK_EQ(tsTime, tmval);

            CHECK_EQ(ddb::Interval::GetMilli(ival), rowCount * 3'600'000);
        }));

        CHECK_EQ(rowCount, numRows);
    }

    SUBCASE("duckforeach year_month_day, hh_mm_ss, timestamp")
    {
        dfe::DuckForEach dfe{con.Query("select dtval, tmval, tsval from t")};

        unsigned rowCount{0};
        CHECK_NOTHROW(dfe([&](dfe::year_month_day ymd, dfe::hh_mm_ss hms, dfe::Timestamp ts) {
            ++rowCount;

            dfe::year_month_day expectedYmd{chr::year{2024}, chr::month{6}, chr::day{rowCount}};
            CHECK_EQ(ymd, expectedYmd);

            CHECK_EQ(hms.hours(), chr::hours{11});
            CHECK_EQ(hms.minutes(), chr::minutes{30});
            CHECK_EQ(hms.seconds(), chr::seconds{rowCount});

            CHECK_EQ(ts.ymd, expectedYmd);
            CHECK_EQ(ts.hms.hours(), chr::hours{11});
            CHECK_EQ(ts.hms.minutes(), chr::minutes{30});
            CHECK_EQ(ts.hms.seconds(), chr::seconds{rowCount});
        }));
    }

    SUBCASE("string conversions")
    {
        dfe::DuckForEach dfe{con.Query("select dtval, tmval, tsval, ival from t")};
        size_t rowCount{0};
        CHECK_NOTHROW(
            dfe([&](std::string dtval, std::string tmval, std::string tsval, std::string ival) {
                ++rowCount;
                CHECK_EQ(dtval, std::format("2024-06-{:02}", rowCount));
                CHECK_EQ(tmval, std::format("11:30:{:02}", rowCount));
                CHECK_EQ(tsval, std::format("2024-06-{0:02} 11:30:{0:02}", rowCount));
                CHECK_EQ(ival, std::format("{0:02}:00:00", rowCount));
            }));
    }

    SUBCASE("handle nulls")
    {
        REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (null, null, null, null)")->HasError());

        SUBCASE("error on null date without optional")
        {
            dfe::DuckForEach dfe{con.Query("select dtval from t")};
            CHECK_THROWS(dfe([&](ddb::date_t dtval) {}));
        }

        SUBCASE("handle null date with optional")
        {
            dfe::DuckForEach dfe{con.Query("select dtval from t")};
            size_t nullCount{0}, rowCount{0};
            CHECK_NOTHROW(dfe([&](std::optional<ddb::date_t> dtval) {
                if (!dtval)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    CHECK_EQ(ddb::Date::ExtractYear(*dtval), 2024);
                    CHECK_EQ(ddb::Date::ExtractMonth(*dtval), 6);
                    CHECK_EQ(ddb::Date::ExtractDay(*dtval), rowCount);
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("error on null time without optional")
        {
            dfe::DuckForEach dfe{con.Query("select tmval from t")};
            CHECK_THROWS(dfe([&](ddb::dtime_t tmval) {}));
        }

        SUBCASE("handle null time with optional")
        {
            dfe::DuckForEach dfe{con.Query("select tmval from t")};
            size_t nullCount{0}, rowCount{0};
            CHECK_NOTHROW(dfe([&](std::optional<ddb::dtime_t> tmval) {
                if (!tmval)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    int32_t hour, mins, secs, micros;
                    ddb::Time::Convert(*tmval, hour, mins, secs, micros);
                    CHECK_EQ(hour, 11);
                    CHECK_EQ(mins, 30);
                    CHECK_EQ(secs, rowCount);
                    CHECK_EQ(micros, 0);
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("error on null timestamp without optional")
        {
            dfe::DuckForEach dfe{con.Query("select tsval from t")};
            CHECK_THROWS(dfe([&](ddb::timestamp_t tsval) {}));
        }

        SUBCASE("handle null timestamp with optional")
        {
            dfe::DuckForEach dfe{con.Query("select tsval from t")};
            unsigned nullCount{0}, rowCount{0};
            CHECK_NOTHROW(dfe([&](std::optional<ddb::timestamp_t> tsval) {
                namespace chr = std::chrono;
                if (!tsval)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    ddb::date_t tsDate;
                    ddb::dtime_t tsTime;
                    ddb::Timestamp::Convert(*tsval, tsDate, tsTime);

                    CHECK_EQ(ddb::Date::ExtractYear(tsDate), 2024);
                    CHECK_EQ(ddb::Date::ExtractMonth(tsDate), 6);
                    CHECK_EQ(ddb::Date::ExtractDay(tsDate), rowCount);

                    int32_t hour, mins, secs, micros;
                    ddb::Time::Convert(tsTime, hour, mins, secs, micros);
                    CHECK_EQ(hour, 11);
                    CHECK_EQ(mins, 30);
                    CHECK_EQ(secs, rowCount);
                    CHECK_EQ(micros, 0);
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("error on null interval without optional")
        {
            dfe::DuckForEach dfe{con.Query("select ival from t")};
            CHECK_THROWS(dfe([&](ddb::interval_t dtval) {}));
        }

        SUBCASE("handle null interval with optional")
        {
            dfe::DuckForEach dfe{con.Query("select ival from t")};
            size_t nullCount{0}, rowCount{0};
            CHECK_NOTHROW(dfe([&](std::optional<ddb::interval_t> ival) {
                if (!ival)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    CHECK_EQ(ddb::Interval::GetMilli(*ival), rowCount * 3'600'000);
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("year_month_day without optional")
        {
            dfe::DuckForEach dfe{con.Query("select dtval from t")};
            CHECK_THROWS(dfe([&](dfe::year_month_day ymd) {}));
        }

        SUBCASE("year_month_day with optional")
        {
            dfe::DuckForEach dfe{con.Query("select dtval from t")};
            unsigned nullCount{0}, rowCount{0};

            CHECK_NOTHROW(dfe([&](std::optional<dfe::year_month_day> ymd) {
                if (!ymd)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    dfe::year_month_day expectedYmd{chr::year{2024}, chr::month{6},
                                                    chr::day{rowCount}};
                    CHECK_EQ(*ymd, expectedYmd);
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("hh_mm_ss without optional")
        {
            dfe::DuckForEach dfe{con.Query("select tmval from t")};
            CHECK_THROWS(dfe([&](dfe::hh_mm_ss hms) {}));
        }

        SUBCASE("hh_mm_ss with optional")
        {
            dfe::DuckForEach dfe{con.Query("select tmval from t")};
            unsigned nullCount{0}, rowCount{0};

            CHECK_NOTHROW(dfe([&](std::optional<dfe::hh_mm_ss> hms) {
                if (!hms)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    CHECK_EQ(hms->hours(), chr::hours{11});
                    CHECK_EQ(hms->minutes(), chr::minutes{30});
                    CHECK_EQ(hms->seconds(), chr::seconds{rowCount});
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }

        SUBCASE("timestamp without optional")
        {
            dfe::DuckForEach dfe{con.Query("select tsval from t")};
            CHECK_THROWS(dfe([&](dfe::Timestamp ts) {}));
        }

        SUBCASE("timestamp with optional")
        {
            dfe::DuckForEach dfe{con.Query("select tsval from t")};
            unsigned nullCount{0}, rowCount{0};

            CHECK_NOTHROW(dfe([&](std::optional<dfe::Timestamp> ts) {
                if (!ts)
                    ++nullCount;
                else
                {
                    ++rowCount;
                    dfe::year_month_day expectedYmd{chr::year{2024}, chr::month{6},
                                                    chr::day{rowCount}};
                    CHECK_EQ(ts->ymd, expectedYmd);
                    CHECK_EQ(ts->hms.hours(), chr::hours{11});
                    CHECK_EQ(ts->hms.minutes(), chr::minutes{30});
                    CHECK_EQ(ts->hms.seconds(), chr::seconds{rowCount});
                }
            }));

            CHECK_EQ(nullCount, 1);
            CHECK_EQ(rowCount, numRows);
        }
    }
}
