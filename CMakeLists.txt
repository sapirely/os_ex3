cmake_minimum_required(VERSION 3.8)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES MapReduceClient.cpp MapReduceClient.h MapReduceFramework.cpp
        MapReduceFramework.h bigClient.cpp Barrier.h Barrier.cpp)
# running littleClient at the moment

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

add_executable(os_ex3 ${SOURCE_FILES})