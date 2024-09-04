cmake_minimum_required(VERSION 3.22)

project(SPARSE_INDEX_BENCHMARK)

file(GLOB BENCHMARK_ENTRY ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
file(GLOB BENCHMARKS ${CMAKE_CURRENT_SOURCE_DIR}/*)

message(STATUS "Config Google Benchmark")

add_executable(sparse_index_benchmark ${BENCHMARK_ENTRY} ${BENCHMARKS})
target_include_directories(sparse_index_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(sparse_index_benchmark
    PUBLIC
    sparse_index_contrib_lib
    rust::sparse_index
    "-framework CoreFoundation"
)
