// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "duckforeach.hpp"

#include <format>
#include <iostream>

namespace ddb = duckdb;
namespace dfe = duckforeach;

int main(int argc, char* argv[])
{
    try
    {
        ddb::DuckDB db;
        ddb::Connection con{db};

        con.Query("CREATE TABLE prices(symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT);"
                  "INSERT INTO prices VALUES('AAPL','2024-06-20',209.67,55790688);"
                  "INSERT INTO prices VALUES('AAPL','2024-06-21',207.48,67962787);"
                  "INSERT INTO prices VALUES('NVDA','2024-06-20',130.78,377901573);"
                  "INSERT INTO prices VALUES('NVDA','2024-06-21',126.56,324484624);"
                  "INSERT INTO prices VALUES('TSLA','2024-06-20',181.56,41533612);"
                  "INSERT INTO prices VALUES('TSLA','2024-06-21',183.00,39706710);");

        dfe::DuckForEach f{con.Query("select date, symbol, close, volume "
                                     "from prices "
                                     "order by date")};
        f([&](dfe::Timestamp ts, std::string sym, double close, int64_t volume) {
            std::cout << std::format("{:%F} {} {:.2f} {:9}", ts, sym, close, volume) << std::endl;
        });
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}
