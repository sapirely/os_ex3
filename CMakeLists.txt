cmake_minimum_required(VERSION 3.8)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES MapReduceClient.cpp MapReduceFramework.cpp)
add_executable(os_ex3 ${SOURCE_FILES})