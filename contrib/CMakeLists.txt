cmake_minimum_required(VERSION 3.22)

project(SPAESE_INDEX_CONTRIB)

add_subdirectory(boost-cmake)
add_subdirectory(google_test)
add_subdirectory(benchmark)
add_subdirectory(corrosion-rs-cmake)

# gather third-party include directories
set(CONTRIB_INCLUDE_DIRS 
    ${CMAKE_CURRENT_SOURCE_DIR}/json/single_include
    ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson/include
    ${CMAKE_CURRENT_SOURCE_DIR}/easylogging/src
    ${CMAKE_CURRENT_SOURCE_DIR}/google_test/googletest
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include
)

# gather third-party sources
file(GLOB_RECURSE CONTRIB_SOURCES 
    # ${CMAKE_CURRENT_SOURCE_DIR}/json/single_include/*
    ${CMAKE_CURRENT_SOURCE_DIR}/easylogging/src/*
    ${CMAKE_CURRENT_SOURCE_DIR}/google_test/googletest/include
    # ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/src/*
    )

# create a library
message(STATUS "Create sparse_index contrib library")
add_library(sparse_index_contrib_lib ${CONTRIB_SOURCES})
target_link_libraries(sparse_index_contrib_lib PUBLIC boost::program_options benchmark::benchmark)
target_include_directories(sparse_index_contrib_lib PUBLIC ${CONTRIB_INCLUDE_DIRS})