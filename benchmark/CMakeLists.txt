find_package(benchmark REQUIRED)

add_executable(bench bench.cpp)

target_link_libraries(bench part benchmark::benchmark)