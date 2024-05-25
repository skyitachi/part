#find_package(benchmark REQUIRED)

#add_executable(bench bench.cpp)
#target_link_libraries(bench part benchmark::benchmark)
#
#add_executable(bench_serialize bench_serialize.cpp)
#target_link_libraries(bench_serialize part)

set(COMMON_SOURCE util.cpp)

add_executable(bench_index bench_multi_index.cpp)
target_link_libraries(bench_index PRIVATE hamt)

add_executable(bench_hamt ${COMMON_SOURCE} bench_hamt.cpp)
target_link_libraries(bench_hamt PRIVATE hamt gc uuid dlmalloc)

add_executable(bench_art bench.cpp)
target_link_libraries(bench_art PRIVATE part)

add_executable(bench_part ${COMMON_SOURCE} bench_part.cpp)
target_link_libraries(bench_part PRIVATE part)