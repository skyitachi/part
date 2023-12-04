cmake_minimum_required(VERSION 3.10)

project(part)

set(CMAKE_CXX_STANDARD 20)

include_directories(src/include /usr/local/include)

set(SRC_FILES
    src/art_key.cpp
    src/allocator.cpp
    src/arena_allocator.cpp
)

add_library(part SHARED ${SRC_FILES})

target_link_libraries(part fmt)

add_subdirectory(examples)