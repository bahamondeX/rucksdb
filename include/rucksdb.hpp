#pragma once

#include <string>
#include <functional>

// RucksDB public API
namespace rucksdb {

// Initialize RucksDB with given path
extern "C" void init(const char* db_path = nullptr);

// Shutdown RucksDB
extern "C" void shutdown();

// Key-value operations
bool put(const std::string& key, const std::string& value);
bool get(const std::string& key, std::string& value);
bool del(const std::string& key);

// Iteration
void scan_prefix(const std::string& prefix, 
                std::function<bool(const std::string& key, const std::string& value)> callback);

} // namespace rucksdb