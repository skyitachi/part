cmake_minimum_required(VERSION 3.10)

project(part)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)

include_directories(include /usr/local/include)
link_directories(/usr/local/lib)

set(SRC_FILES
        src/art_key.cpp
        src/allocator.cpp
        src/arena_allocator.cpp
        src/art.cpp
        src/prefix.cpp
        src/leaf.cpp
        src/fixed_size_allocator.cpp
        src/node.cpp
        src/node4.cpp
        src/node16.cpp
        src/node48.cpp
        src/node256.cpp
        src/serializer.cpp
        src/helper.cpp
        src/concurrent_node.cpp
        src/concurrent_art.cpp
)

add_library(part SHARED ${SRC_FILES})

target_link_libraries(part fmt backward)

add_subdirectory(examples)

include_directories(thirdparty/googletest/googletest/include)

add_subdirectory(thirdparty/googletest)
add_subdirectory(thirdparty/benchmark)

add_subdirectory(tests)

add_subdirectory(benchmark)
