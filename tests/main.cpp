// Copyright (C) 2024 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include "duckforeach.hpp"

namespace ddb = duckdb;
namespace dfe = duckforeach;

TEST_CASE("Check constructors")
{
    CHECK_THROWS(dfe::for_each(nullptr, [](int16_t) {}));

    ddb::DuckDB db;
    ddb::Connection con{db};
    // Should throw if Query returns a result with an error.
    CHECK_THROWS(dfe::for_each(con.Query("select * from notable"), [](bool b) {}));
}