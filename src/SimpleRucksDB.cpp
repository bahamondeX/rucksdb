#include "../include/SimpleRucksDB.hpp"
#include <iostream>

namespace duckdb {

// Global simple registry
std::unique_ptr<SimpleTableRegistry> g_simple_registry;

// Extension implementation
void SimpleRucksDBExtension::Load(DuckDB &db) {
    std::cout << "🚀 Loading Simple RucksDB Extension..." << std::endl;
    
    // Initialize registry if storage is available
    if (g_rocksdb_storage && !g_simple_registry) {
        g_simple_registry = std::make_unique<SimpleTableRegistry>(g_rocksdb_storage.get());
        std::cout << "✅ Simple RucksDB registry initialized" << std::endl;
    }
}

// Simple table registry implementation
void SimpleTableRegistry::CreateSimpleTable(const std::string& name) {
    std::string key = "table_meta_" + name;
    std::string value = "created";
    storage_->WriteData(key, value);
}

void SimpleTableRegistry::DropSimpleTable(const std::string& name) {
    std::string meta_key = "table_meta_" + name;
    storage_->DeleteData(meta_key);
    
    // Delete all data for this table
    std::string prefix = "table_data_" + name + "_";
    storage_->IteratePrefix(prefix, [this](const std::string& key, const std::string& value) {
        storage_->DeleteData(key);
        return true;
    });
}

bool SimpleTableRegistry::TableExists(const std::string& name) {
    std::string key = "table_meta_" + name;
    std::string value;
    return storage_->ReadData(key, value);
}

void SimpleTableRegistry::InsertData(const std::string& table_name, const std::string& key, const std::string& value) {
    if (!TableExists(table_name)) {
        throw std::runtime_error("Table '" + table_name + "' does not exist");
    }
    
    std::string data_key = "table_data_" + table_name + "_" + key;
    storage_->WriteData(data_key, value);
}

bool SimpleTableRegistry::ReadData(const std::string& table_name, const std::string& key, std::string& value) {
    if (!TableExists(table_name)) {
        return false;
    }
    
    std::string data_key = "table_data_" + table_name + "_" + key;
    return storage_->ReadData(data_key, value);
}

std::vector<std::string> SimpleTableRegistry::ListTables() {
    std::vector<std::string> tables;
    std::string prefix = "table_meta_";
    
    storage_->IteratePrefix(prefix, [&tables, &prefix](const std::string& key, const std::string& value) {
        std::string table_name = key.substr(prefix.length());
        tables.push_back(table_name);
        return true;
    });
    
    return tables;
}

} // namespace duckdb