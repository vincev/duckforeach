set(CMAKE_COMPILE_WARNING_AS_ERROR YES)

# Configure test exectuable.
add_executable(duckforeach_tests
    main.cpp
    int_types.cpp
    float_types.cpp
)

target_link_libraries(duckforeach_tests
    PRIVATE duckforeach
    PRIVATE doctest
)

add_test(NAME duckforeach_tests COMMAND duckforeach_tests)
