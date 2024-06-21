set(CMAKE_CXX_STANDARD 17)

add_library(duckdb STATIC
    src/duckdb-1.cpp
    src/duckdb-2.cpp
    src/duckdb-3.cpp
    src/duckdb-4.cpp
    src/duckdb-5.cpp
    src/duckdb-fastpforlib.cpp
    src/duckdb-fmt.cpp
    src/duckdb-fsst.cpp
    src/duckdb-hyperloglog.cpp
    src/duckdb-libpg_query.cpp
    src/duckdb-mbedtls.cpp
    src/duckdb-miniz.cpp
    src/duckdb-re2.cpp
    src/duckdb-skiplist.cpp
    src/duckdb-utf8proc.cpp
    src/duckdb-yyjson.cpp
)

target_include_directories(duckdb
    PUBLIC include
)
