set(CMAKE_COMPILE_WARNING_AS_ERROR YES)

add_library(duckforeach INTERFACE)

target_include_directories(duckforeach
    INTERFACE include
)

target_link_libraries(duckforeach
    INTERFACE duckdb
)
