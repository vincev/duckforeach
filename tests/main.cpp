// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include "duckforeach.hpp"

namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Check constructors")
{
    CHECK_THROWS(dfe::DuckForEach{nullptr});

    ddb::DuckDB db;
    ddb::Connection con{db};

    // Should throw if Query returns a result with an error.
    auto res{con.Query("select * from notable")};
    CHECK_THROWS(dfe::DuckForEach{std::move(res)});
}