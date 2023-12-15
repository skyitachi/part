include_directories(../include)

add_executable(test_basics test_basics.cpp)

target_link_libraries(test_basics gtest gtest_main part fmt)