// src/RocksDBStorage.cpp

#include "../include/RocksDBStorage.hpp"
#include <functional>
#include <iostream>

namespace duckdb {

// Global storage instance
std::unique_ptr<RocksDBStorage> g_rocksdb_storage;

// RocksDBStorage implementation
RocksDBStorage::RocksDBStorage(const string &path) : db_path_(path + "_rocksdb") {}

RocksDBStorage::~RocksDBStorage() {
    if (db_) {
        db_->Close();
    }
}

void RocksDBStorage::Initialize() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    
    rocksdb::DB* db_raw;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_raw);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    
    db_.reset(db_raw);
}

void RocksDBStorage::WriteData(const string &key, const string &value) {
    auto status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("RocksDB write failed: " + status.ToString());
    }
}

bool RocksDBStorage::ReadData(const string &key, string &value) {
    auto status = db_->Get(rocksdb::ReadOptions(), key, &value);
    return status.ok();
}

void RocksDBStorage::DeleteData(const string &key) {
    db_->Delete(rocksdb::WriteOptions(), key);
}

void RocksDBStorage::IteratePrefix(const string &prefix, 
                                 std::function<bool(const string&, const string&)> callback) {
    auto it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        string key = it->key().ToString();
        string value = it->value().ToString();
        if (!callback(key, value)) {
            break;
        }
    }
    delete it;
}

void RocksDBStorage::CreateTable(const string &table_name) {
    table_row_counts_[table_name] = 0;
}

void RocksDBStorage::DropTable(const string &table_name) {
    string prefix = "table_" + table_name + "_";
    IteratePrefix(prefix, [this](const string &key, const string &value) {
        DeleteData(key);
        return true;
    });
    table_row_counts_.erase(table_name);
}

size_t RocksDBStorage::GetTableRowCount(const string &table_name) {
    auto it = table_row_counts_.find(table_name);
    return it != table_row_counts_.end() ? it->second : 0;
}

void RocksDBStorage::SetTableRowCount(const string &table_name, size_t count) {
    table_row_counts_[table_name] = count;
}

} // namespace duckdb

// C interface for extension
extern "C" {
void rucksdb_init(const char* db_path) {
    duckdb::g_rocksdb_storage = std::make_unique<duckdb::RocksDBStorage>(db_path ? db_path : "./rucksdb_data");
    duckdb::g_rocksdb_storage->Initialize();
}

void rucksdb_shutdown() {
    duckdb::g_rocksdb_storage.reset();
}
}