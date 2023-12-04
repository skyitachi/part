cmake_minimum_required(VERSION 3.10)

project(part)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(src/include /usr/local/include)
link_directories(/usr/local/lib)

set(SRC_FILES
        src/art_key.cpp
        src/allocator.cpp
        src/arena_allocator.cpp
        src/art.cpp
        src/prefix.cpp
        src/leaf.cpp
        src/fixed_size_allocator.cpp
)

add_library(part SHARED ${SRC_FILES})

target_link_libraries(part fmt)

add_subdirectory(examples)