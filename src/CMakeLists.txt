set(CMAKE_COMPILE_WARNING_AS_ERROR YES)

add_library(duckforeach INTERFACE)

target_include_directories(duckforeach
    INTERFACE include
)

target_link_libraries(duckforeach
    INTERFACE duckdb
)

# Configure test exectuable.
add_executable(duckforeach_tests
    tests/main.cpp
)

target_link_libraries(duckforeach_tests
    PRIVATE duckforeach
    PRIVATE doctest
)

add_test(NAME duckforeach_tests COMMAND duckforeach_tests)
