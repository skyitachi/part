include_directories(../include)

add_compile_options(-fsanitize=address)

add_executable(test_basics test_basics.cpp)

target_link_libraries(test_basics gtest gtest_main part fmt)

add_executable(test_serialize test_serialize.cpp)

target_link_libraries(test_serialize gtest gtest_main part fmt)

add_executable(test_concurrent_art test_concurrent_art.cpp)
target_link_libraries(test_concurrent_art gtest gtest_main part fmt)
