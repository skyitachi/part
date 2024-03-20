find_package(benchmark REQUIRED)

add_executable(bench bench.cpp)
target_link_libraries(bench part benchmark::benchmark)

add_executable(bench_serialize bench_serialize.cpp)
target_link_libraries(bench_serialize part)