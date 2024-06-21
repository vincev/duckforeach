// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#include "duckforeach.hpp"

#include "duckdb.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <format>
#include <iostream>

int main(int argc, char* argv[])
{
    namespace fs = std::filesystem;

    if (argc < 2)
    {
        auto appName{fs::path{argv[0]}.filename().string()};
        std::cerr << "Usage: " << appName << " [Database path]" << std::endl;
        return EXIT_FAILURE;
    }

    fs::path dbPath{argv[1]};
    if (!exists(dbPath))
    {
        std::cerr << "DB file " << dbPath << " not found." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        duckdb::DuckDB db{dbPath.c_str()};
        duckdb::Connection con{db};

        auto qResult{con.Query("select timestamp, symbol, open, close from prices limit 5")};
        duckforeach::DuckForEach forEach{std::move(qResult)};
        forEach([](const std::string& ts, const std::optional<std::string>& sym, double open,
                   double close) {
            std::cout << ts << " " << sym.value() << " " << open << " " << close << std::endl;
        });
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}