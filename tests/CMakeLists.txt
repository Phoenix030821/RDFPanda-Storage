project(Storage_Tests)

# 添加 Google Test 子目录
add_subdirectory(lib)

# 包含 Google Test 头文件目录
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# 添加测试文件
add_executable(Storage_Tests test_input_parser.cpp ../InputParser.cpp)

# 链接 Google Test 库
target_link_libraries(Storage_Tests gtest gtest_main)