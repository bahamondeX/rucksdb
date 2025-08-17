cmake_minimum_required(VERSION 3.15)
project(rucksdb CXX)

include(${CMAKE_BINARY_DIR}/conan/conan_toolchain.cmake)

find_package(duckdb REQUIRED)
find_package(RocksDB REQUIRED)

# Add your library with simple integration
add_library(rucksdb STATIC 
    src/rucksdb.cpp
    src/RocksDBStorage.cpp
    src/SimpleRucksDB.cpp
)

target_link_libraries(rucksdb PUBLIC
    duckdb::duckdb
    RocksDB::rocksdb
)

target_include_directories(rucksdb PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Set C++17 standard (required for both DuckDB and RocksDB)
set_target_properties(rucksdb PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Add executable
add_executable(rucksdb_example src/main.cpp)

target_link_libraries(rucksdb_example PRIVATE rucksdb)

set_target_properties(rucksdb_example PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

install(TARGETS rucksdb_example DESTINATION bin)