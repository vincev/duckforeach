// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "duckdb.hpp"

#include <chrono>
#include <format>
#include <functional>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>

namespace duckforeach {

namespace details {
template <typename... Cols, typename DbRow> auto castRow(const DbRow& dbRow);
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
                std::format("Invalid number of arguments, function has {} but query result has {}",
                            sizeof...(Args), nCols)};

        for (auto rowsIt{mResult->begin()}; rowsIt != mResult->end(); ++rowsIt)
        {
            std::apply(f, details::castRow<Args...>(*rowsIt));
        }
    }

    std::unique_ptr<duckdb::QueryResult> mResult;
};

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

} // namespace details

} // namespace duckforeach