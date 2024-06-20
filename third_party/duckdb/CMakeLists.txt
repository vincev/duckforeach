set(CMAKE_CXX_STANDARD 17)

add_library(duckdb STATIC
    src/duckdb.cpp
)

target_include_directories(duckdb
    PUBLIC include
)
