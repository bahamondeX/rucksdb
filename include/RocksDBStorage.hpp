// include/RocksDBStorage.hpp
#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <functional>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace duckdb {

using string = std::string;

class RocksDBStorage {
private:
    std::unique_ptr<rocksdb::DB> db_;
    string db_path_;
    std::unordered_map<string, size_t> table_row_counts_;
    
public:
    RocksDBStorage(const string &path);
    ~RocksDBStorage();
    
    void Initialize();
    
    // Storage operations
    void WriteData(const string &key, const string &value);
    bool ReadData(const string &key, string &value);
    void DeleteData(const string &key);
    void IteratePrefix(const string &prefix, 
                      std::function<bool(const string&, const string&)> callback);
    
    // Table management
    void CreateTable(const string &table_name);
    void DropTable(const string &table_name);
    size_t GetTableRowCount(const string &table_name);
    void SetTableRowCount(const string &table_name, size_t count);
    
    rocksdb::DB* GetDB() { return db_.get(); }
};

// Global storage instance
extern std::unique_ptr<RocksDBStorage> g_rocksdb_storage;

} // namespace duckdb