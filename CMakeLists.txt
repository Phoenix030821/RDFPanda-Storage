cmake_minimum_required(VERSION 3.24)
project(RDFPanda_Storage)

set(CMAKE_CXX_STANDARD 17)

add_executable(RDFPanda_Storage main.cpp InputParser.h InputParser.cpp)

# 添加测试目录
add_subdirectory(tests)

#add_subdirectory(tests/googletest)
#include_directories(${PROJECT_SOURCE_DIR}/src/include tests/googletest/include)
#link_directories(${PROJECT_SOURCE_DIR}/lib tests/googletest)
#target_link_libraries(${PROJECT_NAME} gtest)
#set(GOOGLETEST_VERSION 1.16.0)