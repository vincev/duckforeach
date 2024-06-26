// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "duckforeach.hpp"

#include <format>
#include <iostream>

namespace ddb = duckdb;
namespace dfe = duckforeach;
namespace chr = std::chrono;

constexpr size_t NUM_ROWS = 10'000'000;

void setup(duckdb::Connection& con)
{
    const std::string syms[]{"APPL", "NVDA", "SPY"};

    ddb::Appender app{con, "prices"};
    for (size_t i{0}; i < NUM_ROWS; ++i)
    {
        auto dbDate{duckdb::Date::FromDate(2024, 6, (i % 25) + 1)};
        auto dbTime{duckdb::Time::FromTime(11, 30, i % 60, i % 1000)};
        auto dbTs{duckdb::Timestamp::FromDatetime(dbDate, dbTime)};
        app.AppendRow(syms[i % size(syms)].c_str(), dbTs, 123.4, 1'234'567);
    }
}

int main(int argc, char* argv[])
{
    try
    {
        duckdb::DuckDB db;
        duckdb::Connection con{db};

        auto r{con.Query("CREATE TABLE prices("
                         "symbol VARCHAR, "
                         "ts TIMESTAMP, "
                         "close DOUBLE, "
                         "volume BIGINT);")};
        if (r->HasError())
            throw std::runtime_error(r->GetError());

        setup(con);

        auto startTime{chr::steady_clock::now()};
        size_t rowCount{0};

        dfe::DuckForEach f{con.SendQuery("select symbol, ts, close, volume "
                                         "from prices "
                                         "order by ts")};
        f([&](std::string&& sym, dfe::Timestamp ts, double close, int64_t volume) { ++rowCount; });

        auto dt{chr::duration<double>{chr::steady_clock::now() - startTime}};
        std::cout << std::format("Processed {} rows in {:.2f}s ({:.0f} rows/sec)", rowCount,
                                 dt.count(), rowCount / dt.count())
                  << std::endl;
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}