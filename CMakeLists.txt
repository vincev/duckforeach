cmake_minimum_required(VERSION 3.24)

project(DuckForEach LANGUAGES C CXX)
enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_subdirectory(examples)
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(third_party)
