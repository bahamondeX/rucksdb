#pragma once

#include "duckdb.hpp"
#include "../include/RocksDBStorage.hpp"

namespace duckdb {

// Simple extension class
class SimpleRucksDBExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override { return "simple_rucksdb"; }
    std::string Version() const override { return "1.0.0"; }
};

// Simple table registry without complex serialization
class SimpleTableRegistry {
private:
    RocksDBStorage* storage_;
    
public:
    SimpleTableRegistry(RocksDBStorage* storage) : storage_(storage) {}
    
    void CreateSimpleTable(const std::string& name);
    void DropSimpleTable(const std::string& name);
    bool TableExists(const std::string& name);
    
    void InsertData(const std::string& table_name, const std::string& key, const std::string& value);
    bool ReadData(const std::string& table_name, const std::string& key, std::string& value);
    
    std::vector<std::string> ListTables();
};

// Global simple registry
extern std::unique_ptr<SimpleTableRegistry> g_simple_registry;

} // namespace duckdb