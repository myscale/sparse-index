cmake_minimum_required(VERSION 3.22)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(SPARSE_INDEX_LIB)

set(SANITIZER "" CACHE STRING "Sanitizer to enable (address, thread, memory)")

message(STATUS ${SANITIZER})
if(SANITIZER STREQUAL "address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    # set(CXXBRIDGE_CXXFLAGS "${CXXBRIDGE_CXXFLAGS} -fsanitize=address")
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -fsanitize=address")
elseif(SANITIZER STREQUAL "thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g -fno-omit-frame-pointer")
    set(CXXBRIDGE_CXXFLAGS "${CXXBRIDGE_CXXFLAGS} -fsanitize=thread")
    # set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -Zsanitizer=thread")
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -fsanitize=thread")
    # set(RUST_CXXFLAGS "${RUST_CXXFLAGS}")
elseif(SANITIZER STREQUAL "memory")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory")
    # set(CXXBRIDGE_CXXFLAGS "${CXXBRIDGE_CXXFLAGS} -fsanitize=memory")
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -fsanitize=memory")
endif()


add_subdirectory(contrib ${CMAKE_CURRENT_BINARY_DIR}/contrib)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(RUST_PROFILE release)
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(RUST_PROFILE relwithdebinfo)
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_PROFILE dev)
else()
    set(RUST_PROFILE release)
endif()

corrosion_import_crate(
    MANIFEST_PATH "Cargo.toml"
    PROFILE ${RUST_PROFILE}
)

set(rust_lib "sparse_index")

set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
set(RUST_CXXFLAGS "${CXXBRIDGE_CXXFLAGS} ${RUST_CXXFLAGS}")
message(STATUS "Config cxx-bridge, use RUST_CXXFLAGS: " ${RUST_CXXFLAGS})

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/build.rs.in" "${CMAKE_CURRENT_SOURCE_DIR}/build.rs" @ONLY)


set(cxx_bridge_binary_folder ${CMAKE_BINARY_DIR}/cargo/build/${Rust_CARGO_TARGET_CACHED}/cxxbridge)
set(cxx_bridge_source_file "src/lib.rs")

set(common_header ${cxx_bridge_binary_folder}/rust/cxx.h)
# sparse_index cxx header and source.
set(sparse_index_header ${cxx_bridge_binary_folder}/${rust_lib}/${cxx_bridge_source_file}.h)
set(sparse_index_source ${cxx_bridge_binary_folder}/${rust_lib}/${cxx_bridge_source_file}.cc)

# get include path
set(sparse_index_include ${CMAKE_CURRENT_SOURCE_DIR}/include)

message(STATUS "❄️ -> common_header: ${sparse_index_header}")
message(STATUS "❄️ -> sparse_index header: ${sparse_index_header}")
message(STATUS "❄️ -> sparse_index source: ${sparse_index_source}")
message(STATUS "❄️ -> sparse_index include: ${sparse_index_include}")

add_custom_command(
    OUTPUT
    ${common_header}
    ${sparse_index_header}
    ${sparse_index_source}
    COMMAND
    DEPENDS ${rust_lib}-static
    COMMENT "Fixing cmake to find source files"
)


add_library(${rust_lib}_cxxbridge
    ${common_header}
    ${sparse_index_header}
    ${sparse_index_source}
)
# cxx bridge compiles such bindings
set_target_properties(${rust_lib}_cxxbridge PROPERTIES COMPILE_FLAGS "${CXXBRIDGE_CXXFLAGS}")
set_target_properties(${rust_lib}_cxxbridge PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(${rust_lib}_cxxbridge
    PUBLIC
    ${sparse_index_include}
)

# Create total target with alias with given namespace
add_library(${rust_lib}-total INTERFACE)
target_link_libraries(${rust_lib}-total
    INTERFACE
    ${rust_lib}_cxxbridge
    ${rust_lib}
)

# for end-user to link into project
add_library(rust::${rust_lib} ALIAS ${rust_lib}-total)

add_subdirectory(benchmark ${CMAKE_CURRENT_BINARY_DIR}/benchmark)
# add_subdirectory(tests ${CMAKE_CURRENT_BINARY_DIR}/tests)