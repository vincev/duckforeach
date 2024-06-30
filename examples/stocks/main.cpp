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

        dfe::for_each(con.Query("select date, symbol, close, volume from prices order by date"),
                      [](dfe::Timestamp date, std::string sym, double close, int64_t vol) {
                          std::cout << std::format("{:%F} {} {:.2f} {:9}\n", date, sym, close, vol);
                      });

        std::cout << "\n";

        // For more complex queries we can move the query result into for_each
        auto result{con.Query("select date, symbol, close, volume "
                              "from prices "
                              "where symbol similar to '.*A$' "
                              "order by date")};
        dfe::for_each(std::move(result),
                      [](dfe::Timestamp date, std::string sym, double close, int64_t vol) {
                          std::cout << std::format("{:%F} {} {:.2f} {:9}\n", date, sym, close, vol);
                      });
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}
