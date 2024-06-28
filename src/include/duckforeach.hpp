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

template <typename V> class ValueCast
{
public:
    ValueCast(std::size_t colNum, duckdb::Value& dbVal, V& v)
    {
        cast(colNum, dbVal, v);
    }

private:
    void cast(std::size_t colNum, duckdb::Value& dbVal, bool& outVal)
    {
        cast(colNum, "bool", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<bool>& outVal)
    {
        cast(colNum, "bool", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, char& outVal)
    {
        if (dbVal.type().id() == duckdb::LogicalTypeId::TINYINT)
        {
            int8_t v;
            cast(colNum, "char", dbVal, v);
            outVal = static_cast<char>(v);
        }
        else if (dbVal.type().id() == duckdb::LogicalTypeId::UTINYINT)
        {
            uint8_t v;
            cast(colNum, "char", dbVal, v);
            outVal = static_cast<char>(v);
        }
        else
        {
            throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                    " to char",
                                                    colNum, dbVal.type().ToString())};
        }
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<char>& outVal)
    {
        outVal.reset();
        if (dbVal.type().id() == duckdb::LogicalTypeId::TINYINT)
        {
            std::optional<int8_t> v;
            cast(colNum, "char", dbVal, v);
            if (v)
                outVal = static_cast<char>(*v);
        }
        else if (dbVal.type().id() == duckdb::LogicalTypeId::UTINYINT)
        {
            std::optional<uint8_t> v;
            cast(colNum, "char", dbVal, v);
            if (v)
                outVal = static_cast<char>(*v);
        }
        else
        {
            throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                    " to char",
                                                    colNum, dbVal.type().ToString())};
        }
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, int8_t& outVal)
    {
        cast(colNum, "int8", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<int8_t>& outVal)
    {
        cast(colNum, "int8", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, int16_t& outVal)
    {
        cast(colNum, "int16", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<int16_t>& outVal)
    {
        cast(colNum, "int16", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, int32_t& outVal)
    {
        cast(colNum, "int32", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<int32_t>& outVal)
    {
        cast(colNum, "int32", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, int64_t& outVal)
    {
        cast(colNum, "int64", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<int64_t>& outVal)
    {
        cast(colNum, "int64", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, uint8_t& outVal)
    {
        cast(colNum, "uint8", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<uint8_t>& outVal)
    {
        cast(colNum, "uint8", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, uint16_t& outVal)
    {
        cast(colNum, "uint16", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<uint16_t>& outVal)
    {
        cast(colNum, "uint16", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, uint32_t& outVal)
    {
        cast(colNum, "uint32", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<uint32_t>& outVal)
    {
        cast(colNum, "uint32", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, uint64_t& outVal)
    {
        cast(colNum, "uint64", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<uint64_t>& outVal)
    {
        cast(colNum, "uint64", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, double& outVal)
    {
        cast(colNum, "double", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<double>& outVal)
    {
        cast(colNum, "double", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, float& outVal)
    {
        cast(colNum, "float", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<float>& outVal)
    {
        cast(colNum, "float", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::string& outVal)
    {
        cast(colNum, "string", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<std::string>& outVal)
    {
        cast(colNum, "string", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, duckdb::date_t& outVal)
    {
        cast(colNum, "date", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<duckdb::date_t>& outVal)
    {
        cast(colNum, "date", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, duckdb::dtime_t& outVal)
    {
        cast(colNum, "time", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<duckdb::dtime_t>& outVal)
    {
        cast(colNum, "time", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, duckdb::timestamp_t& outVal)
    {
        cast(colNum, "timestamp", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<duckdb::timestamp_t>& outVal)
    {
        cast(colNum, "timestamp", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, duckdb::interval_t& outVal)
    {
        cast(colNum, "interval", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<duckdb::interval_t>& outVal)
    {
        cast(colNum, "interval", dbVal, outVal);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, Timestamp& outVal)
    {
        namespace ddb = duckdb;

        ddb::timestamp_t ddbts;
        cast(colNum, "Timestamp", dbVal, ddbts);

        ddb::date_t ddbDate;
        ddb::dtime_t ddbTime;
        ddb::Timestamp::Convert(ddbts, ddbDate, ddbTime);

        year_month_day ymd{cast(ddbDate)};
        hh_mm_ss hms{cast(ddbTime)};
        outVal = Timestamp{std::chrono::sys_days{ymd} + hms.to_duration()};
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<Timestamp>& outVal)
    {
        namespace ddb = duckdb;

        std::optional<ddb::timestamp_t> ddbts;
        cast(colNum, "Timestamp", dbVal, ddbts);
        if (ddbts)
        {
            ddb::date_t ddbDate;
            ddb::dtime_t ddbTime;
            ddb::Timestamp::Convert(*ddbts, ddbDate, ddbTime);

            year_month_day ymd{cast(ddbDate)};
            hh_mm_ss hms{cast(ddbTime)};

            outVal = Timestamp{std::chrono::sys_days{ymd} + hms.to_duration()};
        }
        else
        {
            outVal = std::nullopt;
        }
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, year_month_day& outVal)
    {
        duckdb::date_t ddbDate;
        cast(colNum, "year_month_day", dbVal, ddbDate);
        outVal = cast(ddbDate);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<year_month_day>& outVal)
    {
        std::optional<duckdb::date_t> ddbDate;
        cast(colNum, "year_month_day", dbVal, ddbDate);
        if (ddbDate)
            outVal = cast(*ddbDate);
        else
            outVal = std::nullopt;
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, hh_mm_ss& outVal)
    {
        duckdb::dtime_t ddbTime;
        cast(colNum, "hh_mm_ss", dbVal, ddbTime);
        outVal = cast(ddbTime);
    }

    void cast(std::size_t colNum, duckdb::Value& dbVal, std::optional<hh_mm_ss>& outVal)
    {
        std::optional<duckdb::dtime_t> ddbTime;
        cast(colNum, "hh_mm_ss", dbVal, ddbTime);
        if (ddbTime)
            outVal = cast(*ddbTime);
        else
            outVal = std::nullopt;
    }

    year_month_day cast(duckdb::date_t ddbDate)
    {
        namespace chr = std::chrono;
        namespace ddb = duckdb;

        return chr::year_month_day{
            chr::year(ddb::Date::ExtractYear(ddbDate)),
            chr::month(ddb::Date::ExtractMonth(ddbDate)),
            chr::day(ddb::Date::ExtractDay(ddbDate)),
        };
    }

    hh_mm_ss cast(duckdb::dtime_t ddbTime)
    {
        namespace chr = std::chrono;
        namespace ddb = duckdb;

        int32_t ddbHour{}, ddbMin{}, ddbSec{}, ddbMicros{};
        ddb::Time::Convert(ddbTime, ddbHour, ddbMin, ddbSec, ddbMicros);

        chr::seconds secs{ddbHour * 3600 + ddbMin * 60 + ddbSec};
        chr::microseconds musecs{ddbMicros};
        musecs += secs;

        return hh_mm_ss{chr::duration_cast<chr::nanoseconds>(musecs)};
    }

    template <typename T>
    void cast(std::size_t colNum, const char* typeName, duckdb::Value& dbVal, T& outVal)
    {
        if (dbVal.IsNull())
            throw std::invalid_argument{std::format("Cannot convert null value at column {} to "
                                                    "{} use std::optional for this column",
                                                    colNum, typeName)};
        try
        {
            outVal = dbVal.GetValue<T>();
        }
        catch (const std::exception& e)
        {
            throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                    " to {}",
                                                    colNum, dbVal.type().ToString(), typeName)};
        }
    }

    template <typename T>
    void
    cast(std::size_t colNum, const char* typeName, duckdb::Value& dbVal, std::optional<T>& outVal)
    {
        if (dbVal.IsNull())
            outVal = std::nullopt;
        else
        {
            try
            {
                outVal = dbVal.GetValue<T>();
            }
            catch (const std::exception& e)
            {
                throw std::invalid_argument{
                    std::format("Cannot convert value at column {} of type {}"
                                " to {}",
                                colNum, dbVal.type().ToString(), typeName)};
            }
        }
    }
};

template <std::size_t ColIdx, typename DbRow, typename... Cols>
void castValue(const DbRow& dbRow, std::tuple<Cols...>& outRow)
{
    auto dbVal{dbRow.iterator.chunk->GetValue(ColIdx, dbRow.row)};
    auto& outVal{std::get<ColIdx>(outRow)};

    ValueCast{ColIdx + 1, dbVal, outVal};

    if constexpr (ColIdx + 1 < sizeof...(Cols))
        castValue<ColIdx + 1>(dbRow, outRow);
}

template <typename... Cols, typename DbRow> auto castRow(const DbRow& dbRow)
{
    std::tuple<std::decay_t<Cols>...> outRow;
    castValue<0>(dbRow, outRow);
    return outRow;
}

template <typename T>
inline constexpr bool IsValidArgumentV =
    std::is_convertible_v<T, bool> || std::is_convertible_v<T, std::optional<bool>> ||
    std::is_convertible_v<T, char> || std::is_convertible_v<T, std::optional<char>> ||
    std::is_convertible_v<T, int8_t> || std::is_convertible_v<T, std::optional<int8_t>> ||
    std::is_convertible_v<T, int16_t> || std::is_convertible_v<T, std::optional<int16_t>> ||
    std::is_convertible_v<T, int32_t> || std::is_convertible_v<T, std::optional<int32_t>> ||
    std::is_convertible_v<T, int64_t> || std::is_convertible_v<T, std::optional<int64_t>> ||
    std::is_convertible_v<T, uint8_t> || std::is_convertible_v<T, std::optional<uint8_t>> ||
    std::is_convertible_v<T, uint16_t> || std::is_convertible_v<T, std::optional<uint16_t>> ||
    std::is_convertible_v<T, uint32_t> || std::is_convertible_v<T, std::optional<uint32_t>> ||
    std::is_convertible_v<T, uint64_t> || std::is_convertible_v<T, std::optional<uint64_t>> ||
    std::is_convertible_v<T, double> || std::is_convertible_v<T, std::optional<double>> ||
    std::is_convertible_v<T, float> || std::is_convertible_v<T, std::optional<float>> ||
    std::is_convertible_v<T, std::string> || std::is_convertible_v<T, std::optional<std::string>> ||
    std::is_convertible_v<T, duckdb::date_t> ||
    std::is_convertible_v<T, std::optional<duckdb::date_t>> ||
    std::is_convertible_v<T, duckdb::dtime_t> ||
    std::is_convertible_v<T, std::optional<duckdb::dtime_t>> ||
    std::is_convertible_v<T, duckdb::timestamp_t> ||
    std::is_convertible_v<T, std::optional<duckdb::timestamp_t>> ||
    std::is_convertible_v<T, duckdb::interval_t> ||
    std::is_convertible_v<T, std::optional<duckdb::interval_t>> ||
    std::is_convertible_v<T, Timestamp> || std::is_convertible_v<T, std::optional<Timestamp>> ||
    std::is_convertible_v<T, year_month_day> ||
    std::is_convertible_v<T, std::optional<year_month_day>> || std::is_convertible_v<T, hh_mm_ss> ||
    std::is_convertible_v<T, std::optional<hh_mm_ss>>;

template <typename T, typename... Args> constexpr bool IsValidSignature()
{
    constexpr bool isValidArgument = IsValidArgumentV<T>;

    static_assert(isValidArgument, "Invalid argument type T");

    if constexpr (isValidArgument && sizeof...(Args) > 0)
        return IsValidSignature<Args...>();
    else
        return isValidArgument;
}

} // namespace details

class DuckForEach
{
public:
    DuckForEach(std::unique_ptr<duckdb::QueryResult> result)
        : mResult{std::move(result)}
    {
        if (!mResult)
            throw std::invalid_argument{"Invalid query result."};

        if (mResult->HasError())
            throw std::runtime_error(std::format("Query error {}", mResult->GetError()));
    }

    template <typename F> auto operator()(F f)
    {
        return invokeImpl<F>(std::function{f});
    }

private:
    template <typename F, typename R, typename... Args>
    auto invokeImpl(std::function<R(Args...)>&& f)
    {
        if constexpr (details::IsValidSignature<Args...>())
        {
            const uint64_t nCols{mResult->ColumnCount()};

            if (sizeof...(Args) != nCols)
                throw std::invalid_argument{std::format(
                    "Invalid number of arguments, function has {} but query result has {}",
                    sizeof...(Args), nCols)};

            for (auto rowsIt{mResult->begin()}; rowsIt != mResult->end(); ++rowsIt)
            {
                std::apply(f, details::castRow<Args...>(*rowsIt));
            }
        }

        return *f.template target<F>();
    }

    std::unique_ptr<duckdb::QueryResult> mResult;
};

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