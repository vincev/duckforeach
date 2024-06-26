add_executable(stocks
    main.cpp
)

target_link_libraries(stocks
    PRIVATE duckforeach
)