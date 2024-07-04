// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "duckdb.hpp"

#include <chrono>
#include <format>
#include <functional>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <type_traits>

namespace duckforeach {

inline constexpr std::tuple VERSION{0, 1, 0};

using year_month_day = std::chrono::year_month_day;
using hh_mm_ss = std::chrono::hh_mm_ss<std::chrono::nanoseconds>;

// A std::chrono based timestamp.
class Timestamp
{
public:
    using TimeType = std::chrono::sys_time<std::chrono::nanoseconds>;

    Timestamp() = default;

    Timestamp(TimeType time)
        : mTime{time}
    {
    }

    year_month_day ymd() const
    {
        return year_month_day{std::chrono::floor<std::chrono::days>(mTime)};
    }

    hh_mm_ss hms() const
    {
        return hh_mm_ss{mTime - std::chrono::floor<std::chrono::days>(mTime)};
    }

    TimeType time() const
    {
        return mTime;
    }

    auto operator<=>(const Timestamp& rhs) const = default;

    static Timestamp now()
    {
        return Timestamp{std::chrono::system_clock::now()};
    }

private:
    TimeType mTime;
};

template <class C, class T>
std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& os, const Timestamp& ts)
{
    os << ts.time();
    return os;
}

namespace details {

template <typename T>
inline void cast_value(std::size_t column, const char* typestr, duckdb::Value& dbval, T& outval)
{
    if (dbval.IsNull())
        throw std::invalid_argument{std::format("Cannot convert null value at column {} to "
                                                "{} use std::optional for this column",
                                                column, typestr)};
    try
    {
        outval = dbval.GetValue<T>();
    }
    catch (const std::exception& e)
    {
        throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                " to {}",
                                                column, dbval.type().ToString(), typestr)};
    }
}

template <typename T>
inline void
cast_value(std::size_t column, const char* typestr, duckdb::Value& dbval, std::optional<T>& outval)
{
    if (dbval.IsNull())
        outval = std::nullopt;
    else
    {
        try
        {
            outval = dbval.GetValue<T>();
        }
        catch (const std::exception& e)
        {
            throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                    " to {}",
                                                    column, dbval.type().ToString(), typestr)};
        }
    }
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, bool& outval)
{
    cast_value(column, "bool", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<bool>& outval)
{
    cast_value(column, "bool", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, int8_t& outval)
{
    cast_value(column, "int8", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<int8_t>& outval)
{
    cast_value(column, "int8", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, int16_t& outval)
{
    cast_value(column, "int16", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<int16_t>& outval)
{
    cast_value(column, "int16", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, int32_t& outval)
{
    cast_value(column, "int32", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<int32_t>& outval)
{
    cast_value(column, "int32", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, int64_t& outval)
{
    cast_value(column, "int64", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<int64_t>& outval)
{
    cast_value(column, "int64", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, uint8_t& outval)
{
    cast_value(column, "uint8", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<uint8_t>& outval)
{
    cast_value(column, "uint8", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, uint16_t& outval)
{
    cast_value(column, "uint16", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<uint16_t>& outval)
{
    cast_value(column, "uint16", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, uint32_t& outval)
{
    cast_value(column, "uint32", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<uint32_t>& outval)
{
    cast_value(column, "uint32", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, uint64_t& outval)
{
    cast_value(column, "uint64", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<uint64_t>& outval)
{
    cast_value(column, "uint64", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, double& outval)
{
    cast_value(column, "double", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<double>& outval)
{
    cast_value(column, "double", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, float& outval)
{
    cast_value(column, "float", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<float>& outval)
{
    cast_value(column, "float", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::string& outval)
{
    cast_value(column, "string", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<std::string>& outval)
{
    cast_value(column, "string", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, duckdb::date_t& outval)
{
    cast_value(column, "date", dbval, outval);
}

inline void
cast_value(std::size_t column, duckdb::Value& dbval, std::optional<duckdb::date_t>& outval)
{
    cast_value(column, "date", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, duckdb::dtime_t& outval)
{
    cast_value(column, "time", dbval, outval);
}

inline void
cast_value(std::size_t column, duckdb::Value& dbval, std::optional<duckdb::dtime_t>& outval)
{
    cast_value(column, "time", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, duckdb::timestamp_t& outval)
{
    cast_value(column, "timestamp", dbval, outval);
}

inline void
cast_value(std::size_t column, duckdb::Value& dbval, std::optional<duckdb::timestamp_t>& outval)
{
    cast_value(column, "timestamp", dbval, outval);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, duckdb::interval_t& outval)
{
    cast_value(column, "interval", dbval, outval);
}

inline void
cast_value(std::size_t column, duckdb::Value& dbval, std::optional<duckdb::interval_t>& outval)
{
    cast_value(column, "interval", dbval, outval);
}

inline year_month_day cast_to_ymd(duckdb::date_t date)
{
    namespace chr = std::chrono;
    namespace ddb = duckdb;

    return chr::year_month_day{
        chr::year(ddb::Date::ExtractYear(date)),
        chr::month(ddb::Date::ExtractMonth(date)),
        chr::day(ddb::Date::ExtractDay(date)),
    };
}

inline hh_mm_ss cast_to_hms(duckdb::dtime_t time)
{
    namespace chr = std::chrono;
    namespace ddb = duckdb;

    int32_t hours{}, mins{}, secs{}, micros{};
    ddb::Time::Convert(time, hours, mins, secs, micros);

    chr::microseconds musecs{micros};
    musecs += chr::seconds{hours * 3600 + mins * 60 + secs};

    return hh_mm_ss{chr::duration_cast<chr::nanoseconds>(musecs)};
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, Timestamp& outval)
{
    namespace chr = std::chrono;
    namespace ddb = duckdb;

    if (dbval.type().id() == duckdb::LogicalTypeId::TIMESTAMP_NS)
    {
        uint64_t epoch;
        cast_value(column, "Timestamp", dbval, epoch);

        ddb::timestamp_t ddbts{ddb::Timestamp::FromEpochNanoSeconds(epoch)};

        ddb::date_t date;
        ddb::dtime_t time;
        ddb::Timestamp::Convert(ddbts, date, time);

        year_month_day ymd{cast_to_ymd(date)};
        hh_mm_ss hms{cast_to_hms(time)};
        auto dnanos{hms.to_duration() + chr::nanoseconds{epoch % 1000}};
        outval = Timestamp{std::chrono::sys_days{ymd} + dnanos};
    }
    else
    {
        ddb::timestamp_t ddbts;
        cast_value(column, "Timestamp", dbval, ddbts);

        ddb::date_t date;
        ddb::dtime_t time;
        ddb::Timestamp::Convert(ddbts, date, time);

        year_month_day ymd{cast_to_ymd(date)};
        hh_mm_ss hms{cast_to_hms(time)};
        outval = Timestamp{std::chrono::sys_days{ymd} + hms.to_duration()};
    }
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<Timestamp>& outval)
{
    if (!dbval.IsNull())
    {
        Timestamp ts;
        cast_value(column, dbval, ts);
        outval = std::move(ts);
    }
    else
    {
        outval = std::nullopt;
    }
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, year_month_day& outval)
{
    duckdb::date_t date;
    cast_value(column, "year_month_day", dbval, date);
    outval = cast_to_ymd(date);
}

inline void
cast_value(std::size_t column, duckdb::Value& dbval, std::optional<year_month_day>& outval)
{
    std::optional<duckdb::date_t> date;
    cast_value(column, "year_month_day", dbval, date);
    if (date)
        outval = cast_to_ymd(*date);
    else
        outval = std::nullopt;
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, hh_mm_ss& outval)
{
    duckdb::dtime_t time;
    cast_value(column, "hh_mm_ss", dbval, time);
    outval = cast_to_hms(time);
}

inline void cast_value(std::size_t column, duckdb::Value& dbval, std::optional<hh_mm_ss>& outval)
{
    std::optional<duckdb::dtime_t> time;
    cast_value(column, "hh_mm_ss", dbval, time);
    if (time)
        outval = cast_to_hms(*time);
    else
        outval = std::nullopt;
}

template <std::size_t ColIdx, typename DbRow, typename... Cols>
void cast_value(const DbRow& dbRow, std::tuple<Cols...>& outRow)
{
    auto dbval{dbRow.iterator.chunk->GetValue(ColIdx, dbRow.row)};
    auto& outval{std::get<ColIdx>(outRow)};

    cast_value(ColIdx + 1, dbval, outval);

    if constexpr (ColIdx + 1 < sizeof...(Cols))
        cast_value<ColIdx + 1>(dbRow, outRow);
}

template <typename... Cols, typename DbRow> auto cast_row(const DbRow& dbRow)
{
    std::tuple<std::decay_t<Cols>...> outRow;
    cast_value<0>(dbRow, outRow);
    return outRow;
}

template <typename T>
inline constexpr bool is_valid_argument_v =
    std::is_same_v<T, bool> || std::is_same_v<T, std::optional<bool>> ||
    std::is_same_v<T, int8_t> || std::is_same_v<T, std::optional<int8_t>> ||
    std::is_same_v<T, int16_t> || std::is_same_v<T, std::optional<int16_t>> ||
    std::is_same_v<T, int32_t> || std::is_same_v<T, std::optional<int32_t>> ||
    std::is_same_v<T, int64_t> || std::is_same_v<T, std::optional<int64_t>> ||
    std::is_same_v<T, uint8_t> || std::is_same_v<T, std::optional<uint8_t>> ||
    std::is_same_v<T, uint16_t> || std::is_same_v<T, std::optional<uint16_t>> ||
    std::is_same_v<T, uint32_t> || std::is_same_v<T, std::optional<uint32_t>> ||
    std::is_same_v<T, uint64_t> || std::is_same_v<T, std::optional<uint64_t>> ||
    std::is_same_v<T, double> || std::is_same_v<T, std::optional<double>> ||
    std::is_same_v<T, float> || std::is_same_v<T, std::optional<float>> ||
    std::is_same_v<T, std::string> || std::is_same_v<T, std::optional<std::string>> ||
    std::is_same_v<T, duckdb::date_t> || std::is_same_v<T, std::optional<duckdb::date_t>> ||
    std::is_same_v<T, duckdb::dtime_t> || std::is_same_v<T, std::optional<duckdb::dtime_t>> ||
    std::is_same_v<T, duckdb::timestamp_t> ||
    std::is_same_v<T, std::optional<duckdb::timestamp_t>> ||
    std::is_same_v<T, duckdb::interval_t> || std::is_same_v<T, std::optional<duckdb::interval_t>> ||
    std::is_same_v<T, Timestamp> || std::is_same_v<T, std::optional<Timestamp>> ||
    std::is_same_v<T, year_month_day> || std::is_same_v<T, std::optional<year_month_day>> ||
    std::is_same_v<T, hh_mm_ss> || std::is_same_v<T, std::optional<hh_mm_ss>>;

template <typename T, typename... Args> constexpr bool is_valid_signature()
{
    constexpr bool is_valid_arg{is_valid_argument_v<std::decay_t<T>>};

    static_assert(is_valid_arg, "Invalid argument type T");

    if constexpr (is_valid_arg && sizeof...(Args) > 0)
        return is_valid_signature<Args...>();
    else
        return is_valid_arg;
}

template <typename F, typename R, typename... Args>
auto for_each_impl(std::unique_ptr<duckdb::QueryResult> result, std::function<R(Args...)>&& f)
{
    if constexpr (details::is_valid_signature<Args...>())
    {
        const uint64_t ncols{result->ColumnCount()};

        if (sizeof...(Args) != ncols)
            throw std::invalid_argument{
                std::format("Invalid number of arguments, function has {} but query result has {}",
                            sizeof...(Args), ncols)};

        for (auto rowit{result->begin()}; rowit != result->end(); ++rowit)
        {
            std::apply(f, details::cast_row<Args...>(*rowit));
        }
    }

    return *f.template target<F>();
}

} // namespace details

template <typename F> auto for_each(std::unique_ptr<duckdb::QueryResult> result, F f)
{
    if (!result)
        throw std::invalid_argument{"Invalid query result."};

    if (result->HasError())
        throw std::runtime_error(std::format("Query error {}", result->GetError()));

    return details::for_each_impl<F>(std::move(result), std::function{f});
}

} // namespace duckforeach

namespace std {

// Formatting for Timestamp
template <> struct formatter<duckforeach::Timestamp> : formatter<duckforeach::Timestamp::TimeType>
{
    auto format(const duckforeach::Timestamp& ts, auto& ctx) const
    {
        return std::formatter<duckforeach::Timestamp::TimeType>::format(ts.time(), ctx);
    }
};

// Hash for timestamp.
template <> struct hash<duckforeach::Timestamp>
{
    std::size_t operator()(const duckforeach::Timestamp& ts) const noexcept
    {
        return ts.time().time_since_epoch().count();
    }
};

} // namespace std