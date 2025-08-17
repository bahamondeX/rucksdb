// src/rucksdb.cpp
#include "../include/RocksDBStorage.hpp"
#include "../include/rucksdb.hpp"
#include <iostream>

// RucksDB API implementation
namespace rucksdb {

bool put(const std::string& key, const std::string& value) {
    if (!duckdb::g_rocksdb_storage) {
        return false;
    }
    
    try {
        duckdb::g_rocksdb_storage->WriteData(key, value);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "RucksDB put error: " << e.what() << std::endl;
        return false;
    }
}

bool get(const std::string& key, std::string& value) {
    if (!duckdb::g_rocksdb_storage) {
        return false;
    }
    
    return duckdb::g_rocksdb_storage->ReadData(key, value);
}

bool del(const std::string& key) {
    if (!duckdb::g_rocksdb_storage) {
        return false;
    }
    
    try {
        duckdb::g_rocksdb_storage->DeleteData(key);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "RucksDB delete error: " << e.what() << std::endl;
        return false;
    }
}

void scan_prefix(const std::string& prefix, 
                std::function<bool(const std::string&, const std::string&)> callback) {
    if (!duckdb::g_rocksdb_storage) {
        return;
    }
    
    duckdb::g_rocksdb_storage->IteratePrefix(prefix, callback);
}

} // namespace rucksdb