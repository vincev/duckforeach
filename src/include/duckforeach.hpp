// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "duckdb.hpp"

#include <chrono>
#include <format>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>

namespace duckforeach {

namespace details {
template <typename... Cols, typename DbRow> auto fromDbRow(const DbRow& dbRow);
}

class DuckForEach
{
public:
    DuckForEach(std::unique_ptr<duckdb::QueryResult> result)
        : mResult{std::move(result)}
    {
        if (mResult->HasError())
            throw std::runtime_error(std::format("Query error {}", mResult->GetError()));
    }

    template <typename Fn> void operator()(Fn op)
    {
        std::function opFn{op};
        invokeImpl(opFn);
    }

private:
    template <typename R, typename... Args> void invokeImpl(std::function<R(Args...)> f)
    {
        const uint64_t nCols{mResult->ColumnCount()};

        if (sizeof...(Args) != nCols)
            throw std::invalid_argument{
                std::format("for_each error: result has {} columns but passed function has {}",
                            nCols, sizeof...(Args))};

        for (auto rowsIt{mResult->begin()}; rowsIt != mResult->end(); ++rowsIt)
        {
            auto args{details::fromDbRow<Args...>(*rowsIt)};
            std::apply(f, args);
        }
    }

    std::unique_ptr<duckdb::QueryResult> mResult;
};

namespace details {

void setValue(std::size_t colNum, duckdb::Value& dbVal, std::string& outVal)
{
    if (dbVal.type().id() != duckdb::LogicalTypeId::VARCHAR)
        throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                " to std::string",
                                                colNum, dbVal.type().ToString())};

    if (dbVal.IsNull())
        throw std::invalid_argument{std::format("Cannot convert null value at column {} to "
                                                "std::string use std::optional for this column",
                                                colNum)};

    outVal = dbVal.GetValue<std::string>();
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, std::optional<std::string>& outVal)
{
    if (dbVal.type().id() != duckdb::LogicalTypeId::VARCHAR)
        throw std::invalid_argument{std::format("Cannot convert value at column {} of type {}"
                                                " to std::string",
                                                colNum, dbVal.type().ToString())};

    if (dbVal.IsNull())
        outVal = std::nullopt;
    else
        outVal = dbVal.GetValue<std::string>();
}

template <typename T>
void setNumericValue(std::size_t colNum, const char* typeName, duckdb::Value& dbVal, T& outVal)
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

void setValue(std::size_t colNum, duckdb::Value& dbVal, int8_t& outVal)
{
    setNumericValue(colNum, "int8", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, int16_t& outVal)
{
    setNumericValue(colNum, "int16", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, int32_t& outVal)
{
    setNumericValue(colNum, "int32", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, int64_t& outVal)
{
    setNumericValue(colNum, "int64", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, uint8_t& outVal)
{
    setNumericValue(colNum, "uint8", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, uint16_t& outVal)
{
    setNumericValue(colNum, "uint16", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, uint32_t& outVal)
{
    setNumericValue(colNum, "uint32", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, uint64_t& outVal)
{
    setNumericValue(colNum, "uint64", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, double& outVal)
{
    setNumericValue(colNum, "double", dbVal, outVal);
}

void setValue(std::size_t colNum, duckdb::Value& dbVal, float& outVal)
{
    setNumericValue(colNum, "float", dbVal, outVal);
}

template <std::size_t ColIdx, typename DbRow, typename... Cols>
void castValue(const DbRow& dbRow, std::tuple<Cols...>& outRow)
{
    auto dbVal{dbRow.iterator.chunk->GetValue(ColIdx, dbRow.row)};
    auto& outVal{std::get<ColIdx>(outRow)};

    setValue(ColIdx + 1, dbVal, outVal);

    if constexpr (ColIdx + 1 < sizeof...(Cols))
        castValue<ColIdx + 1>(dbRow, outRow);
}

template <typename... Cols, typename DbRow> auto fromDbRow(const DbRow& dbRow)
{
    std::tuple<std::decay_t<Cols>...> outRow;
    castValue<0>(dbRow, outRow);
    return outRow;
}

} // namespace details

} // namespace duckforeach