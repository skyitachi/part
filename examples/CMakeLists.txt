include_directories(../include)

add_executable(demo art_demo.cpp)

find_package(absl REQUIRED)

target_link_libraries(demo part absl::flat_hash_map)

add_executable(art_sanitizer art_sanitizer.cpp)

target_link_libraries(art_sanitizer part)