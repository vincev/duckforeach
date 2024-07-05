# DuckForEach [![C++20][cpp-badhe]][cpp-url] [![Build and Test][actions-badge]][actions-url]

A `for_each` algorithm for [DuckDB][duckdb-url] query results with a bit of
metaprogramming to convert DuckDB `Value` types to standard types: (see
[stocks](./examples/stocks/main.cpp) example):

```cpp
namespace ddb = duckdb;
namespace dfe = duckforeach;

ddb::DuckDB db{"stocks.db"};
ddb::Connection con{db};

dfe::for_each(con.Query("select date, symbol, close, volume from prices order by date"),
              [](dfe::Timestamp date, std::string sym, double close, int64_t vol) {
                  std::cout << std::format("{:%F} {} {:.2f} {:9}\n", date, sym, close, vol);
              });
```

```bash
$ ./build/examples/stocks/stocks
2024-06-20 AAPL 209.67  55790688
2024-06-20 NVDA 130.78 377901573
2024-06-20 TSLA 181.56  41533612
2024-06-21 AAPL 207.48  67962787
2024-06-21 NVDA 126.56 324484624
2024-06-21 TSLA 183.00  39706710
```

[actions-badge]: https://github.com/vincev/duckforeach/actions/workflows/ci.yml/badge.svg?branch=main
[actions-url]: https://github.com/vincev/duckforeach/actions/workflows/ci.yml
[cpp-badhe]: https://img.shields.io/badge/C%2B%2B-20-blue.svg
[cpp-url]: https://isocpp.org/std/the-standard
[duckdb-url]: https://github.com/duckdb/duckdb

## Table of Contents

- [Quick start](#quick-start)
- [Usage](#usage)
  - [Numeric types](#numeric-types)
  - [String types](#string-types)
  - [Time types](#time-types)
  - [Function objects](#function-objects)
  - [Errors](#errors)
- [Build and test locally](#build-and-test-locally)

## Quick start

To use `DuckForEach` download [duckforeacg.hpp](src/include/duckforeach.hpp) and add
it to your project, types are under the `duckforeach` namespace, see
[examples](./examples/) and [tests](./tests/) for usage.

## Usage

`for_each` takes two arguments, the first is a `unique_ptr<duckdb::QueryResult>`
returned by one of the query methods in `duckdb::Connection`, and the second is a
function object that is invoked for each row with the row values converted to the
function object arguments type.

Conversions for numeric types, string types, and time types are supported.

### Numeric types

The following numeric types are supported (see [tests](./tests/ints.cpp)):

- Signed integers: `int8_t`, `int16_t`, `int32_t`, `int64_t`
- Unsigned integers: `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t`
- Floating point: `float`, `double`
- Boolean value: `bool` true for non zero values.

Conversions to these types may fail if a column value overflows or if the value is
NULL.

To handle NULL values wrap the argument in a `std::optional`:

```cpp
dfe::for_each(con.Query("select uval from t"),
              [](std::optional<int8_t> uval)
              {
                  if (uval)
                    std::cout << *uval << std::endl;
                  else
                    std::cout << "NULL" << std::endl;
              });
```

### String types

DuckDB provides many conversions to `std::string`, see examples in the
[tests](./tests/strings.cpp).

A `std::string` argument can be a value, an `rvalue` reference or a `const`
reference. For handling NULLs wrap the argument in a `std::optional`.

### Time types

The following time types are supported (see [tests](./tests/times.cpp)):

- DuckDB types: `date_t`, `dtime_t`, `timestamp_t`, `interval_t`
- Chrono types: `chrono::year_month_day`, `chrono::hh_mm_ss<nanoseconds>`
- Timestamp: a small wrapper around `chrono::sys_time<nanoseconds>`.

For handling NULLs wrap the argument in a `std::optional`.

### Function objects

The function object passed to `for_each` can be a lambda function, a function pointer
or a custom function object.

As in the `std::for_each` the function object is passed by value and returned at the
end of the call to access its state (see [tests](./tests/functions.cpp)).

### Errors

`for_each` throws a `std::invalid_argument` exception if a value conversion is not
supported or if the number of arguments in the function object does not match the
number of columns in the results.

At compile time a `static_assert` makes sure that the function object can be invoked
with any of the types listed above.

## Build and test locally

To build the tests and examples you need a recent `cmake` and a `c++ 20` compiler
(the github action uses clang 18.1.3 and gcc 13.2.0 on Ubuntu-24.04).
