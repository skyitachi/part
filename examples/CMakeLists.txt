include_directories(../src/include)

add_executable(demo art_demo.cpp)

target_link_libraries(demo part fmt)