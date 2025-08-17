#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "RocksDBStorage.hpp"
#include <sstream>

namespace duckdb {

// Forward declarations
class RucksDBTableStorage;

// Extension main class
class RucksDBExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override { return "rucksdb"; }
    std::string Version() const override { return "1.0.0"; }
};

// Schema management for RocksDB tables
class RucksDBSchema {
private:
    RocksDBStorage* storage_;
    static constexpr char SCHEMA_PREFIX[] = "schema_";
    static constexpr char TABLE_META_PREFIX[] = "table_meta_";
    
public:
    RucksDBSchema(RocksDBStorage* storage) : storage_(storage) {}
    
    void CreateTable(const string& table_name, const vector<ColumnDefinition>& columns);
    void DropTable(const string& table_name);
    vector<ColumnDefinition> GetTableSchema(const string& table_name);
    bool TableExists(const string& table_name);
    
    // Metadata operations
    void StoreTableMetadata(const string& table_name, idx_t row_count);
    idx_t LoadTableRowCount(const string& table_name);
};

// Columnar storage in RocksDB
class RucksDBColumnarStorage {
private:
    RocksDBStorage* storage_;
    
    string GetColumnKey(const string& table_name, idx_t col_idx, idx_t row_id);
    string GetRowKey(const string& table_name, idx_t row_id);
    
public:
    RucksDBColumnarStorage(RocksDBStorage* storage) : storage_(storage) {}
    
    // Row-based operations (simpler for initial implementation)
    void WriteRow(const string& table_name, idx_t row_id, const DataChunk& chunk, idx_t chunk_row);
    bool ReadRow(const string& table_name, idx_t row_id, DataChunk& result, idx_t result_row, 
                const vector<column_t>& column_ids);
    void DeleteRow(const string& table_name, idx_t row_id);
    
    // Batch operations
    void WriteChunk(const string& table_name, idx_t start_row, const DataChunk& chunk);
    idx_t ReadChunk(const string& table_name, idx_t start_row, idx_t max_count, 
                   DataChunk& result, const vector<column_t>& column_ids);
                   
    // Scan operations
    idx_t ScanRows(const string& table_name, idx_t start_row, idx_t max_count,
                  DataChunk& result, const vector<column_t>& column_ids);
};

// Custom table storage for RocksDB
class RucksDBTableStorage {
private:
    string table_name_;
    RucksDBSchema* schema_;
    RucksDBColumnarStorage* storage_;
    vector<ColumnDefinition> columns_;
    idx_t row_count_;
    
public:
    RucksDBTableStorage(const string& table_name, RucksDBSchema* schema, 
                       RucksDBColumnarStorage* storage);
    
    void Initialize(const vector<ColumnDefinition>& columns);
    
    // Data operations
    void Append(DataChunk& chunk);
    void Delete(const Vector& row_ids, idx_t count);
    void Update(const Vector& row_ids, const vector<column_t>& column_ids, DataChunk& data);
    
    // Scan operations
    void InitializeScan(TableScanState& state, const vector<column_t>& column_ids);
    void Scan(DataChunk& result, TableScanState& state, const vector<column_t>& column_ids);
    
    // Metadata
    idx_t GetRowCount() const { return row_count_; }
    const vector<ColumnDefinition>& GetColumns() const { return columns_; }
    const string& GetTableName() const { return table_name_; }
};

// Scan state for RocksDB tables
struct RucksDBScanState : public TableScanState {
    idx_t current_row = 0;
    idx_t total_rows = 0;
    string table_name;
    vector<column_t> column_ids;
    bool finished = false;
};

// Table function for scanning RocksDB tables
struct RocksDBTableFunction {
    static void RegisterFunction(DatabaseInstance& db);
    
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types, vector<string>& names);
    
    static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context, 
                                                          TableFunctionInitInput& input);
    
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, 
                                                        TableFunctionInitInput& input,
                                                        GlobalTableFunctionState* global_state);
    
    static void Execute(ClientContext& context, TableFunctionInput& data, DataChunk& output);
};

// Bind data for RocksDB table function
struct RocksDBBindData : public TableFunctionData {
    string table_name;
    vector<LogicalType> types;
    vector<string> names;
    RucksDBTableStorage* table_storage;
};

// Global state for RocksDB table function
struct RocksDBGlobalState : public GlobalTableFunctionState {
    string table_name;
    idx_t total_rows;
};

// Global registry for RocksDB tables
class RucksDBTableRegistry {
private:
    std::unordered_map<string, unique_ptr<RucksDBTableStorage>> tables_;
    unique_ptr<RucksDBSchema> schema_;
    unique_ptr<RucksDBColumnarStorage> storage_;
    
public:
    RucksDBTableRegistry(RocksDBStorage* storage);
    
    void CreateTable(const string& name, const vector<ColumnDefinition>& columns);
    void DropTable(const string& name);
    RucksDBTableStorage* GetTable(const string& name);
    bool TableExists(const string& name);
    
    vector<string> ListTables();
};

// Global registry instance
extern unique_ptr<RucksDBTableRegistry> g_table_registry;

} // namespace duckdb

namespace duckdb {

// Forward declarations
class RucksDBTableStorage;
class RucksDBTransaction;

// Extension main class
class RucksDBExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override { return "rucksdb"; }
    std::string Version() const override { return "1.0.0"; }
};

// Schema management for RocksDB tables
class RucksDBSchema {
private:
    RocksDBStorage* storage_;
    static constexpr char SCHEMA_PREFIX[] = "schema_";
    static constexpr char TABLE_META_PREFIX[] = "table_meta_";
    
public:
    RucksDBSchema(RocksDBStorage* storage) : storage_(storage) {}
    
    void CreateTable(const string& table_name, const vector<ColumnDefinition>& columns);
    void DropTable(const string& table_name);
    vector<ColumnDefinition> GetTableSchema(const string& table_name);
    bool TableExists(const string& table_name);
    
    // Metadata operations
    void StoreTableMetadata(const string& table_name, idx_t row_count);
    idx_t LoadTableRowCount(const string& table_name);
};

// Columnar storage in RocksDB
class RucksDBColumnarStorage {
private:
    RocksDBStorage* storage_;
    
    string GetColumnKey(const string& table_name, idx_t col_idx, idx_t row_id);
    string GetRowKey(const string& table_name, idx_t row_id);
    
public:
    RucksDBColumnarStorage(RocksDBStorage* storage) : storage_(storage) {}
    
    // Row-based operations (simpler for initial implementation)
    void WriteRow(const string& table_name, idx_t row_id, const DataChunk& chunk, idx_t chunk_row);
    bool ReadRow(const string& table_name, idx_t row_id, DataChunk& result, idx_t result_row, 
                const vector<column_t>& column_ids);
    void DeleteRow(const string& table_name, idx_t row_id);
    
    // Batch operations
    void WriteChunk(const string& table_name, idx_t start_row, const DataChunk& chunk);
    idx_t ReadChunk(const string& table_name, idx_t start_row, idx_t max_count, 
                   DataChunk& result, const vector<column_t>& column_ids);
                   
    // Scan operations
    idx_t ScanRows(const string& table_name, idx_t start_row, idx_t max_count,
                  DataChunk& result, const vector<column_t>& column_ids);
};

// Custom table storage for RocksDB
class RucksDBTableStorage {
private:
    string table_name_;
    RocksDBSchema* schema_;
    RucksDBColumnarStorage* storage_;
    vector<ColumnDefinition> columns_;
    idx_t row_count_;
    
public:
    RucksDBTableStorage(const string& table_name, RocksDBSchema* schema, 
                       RucksDBColumnarStorage* storage);
    
    void Initialize(const vector<ColumnDefinition>& columns);
    
    // Data operations
    void Append(DataChunk& chunk);
    void Delete(const Vector& row_ids, idx_t count);
    void Update(const Vector& row_ids, const vector<column_t>& column_ids, DataChunk& data);
    
    // Scan operations
    void InitializeScan(TableScanState& state, const vector<column_t>& column_ids);
    void Scan(DataChunk& result, TableScanState& state, const vector<column_t>& column_ids);
    
    // Metadata
    idx_t GetRowCount() const { return row_count_; }
    const vector<ColumnDefinition>& GetColumns() const { return columns_; }
    const string& GetTableName() const { return table_name_; }
};

// Scan state for RocksDB tables
struct RucksDBScanState : public TableScanState {
    idx_t current_row = 0;
    idx_t total_rows = 0;
    string table_name;
    vector<column_t> column_ids;
    bool finished = false;
};

// Table function for scanning RocksDB tables
struct RocksDBTableFunction {
    static void RegisterFunction(DatabaseInstance& db);
    
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types, vector<string>& names);
    
    static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context, 
                                                          TableFunctionInitInput& input);
    
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, 
                                                        TableFunctionInitInput& input,
                                                        GlobalTableFunctionState* global_state);
    
    static void Execute(ClientContext& context, TableFunctionInput& data, DataChunk& output);
};

// Bind data for RocksDB table function
struct RocksDBBindData : public TableFunctionData {
    string table_name;
    vector<LogicalType> types;
    vector<string> names;
    RucksDBTableStorage* table_storage;
};

// Global state for RocksDB table function
struct RocksDBGlobalState : public GlobalTableFunctionState {
    string table_name;
    idx_t total_rows;
};

// Transaction support
class RucksDBTransaction {
private:
    rocksdb::TransactionDB* txn_db_;
    rocksdb::Transaction* txn_;
    bool active_;
    
public:
    RucksDBTransaction(rocksdb::TransactionDB* txn_db);
    ~RucksDBTransaction();
    
    void Begin();
    void Commit();
    void Rollback();
    
    bool Put(const string& key, const string& value);
    bool Get(const string& key, string& value);
    bool Delete(const string& key);
    
    bool IsActive() const { return active_; }
};

// Global registry for RocksDB tables
class RucksDBTableRegistry {
private:
    std::unordered_map<string, unique_ptr<RucksDBTableStorage>> tables_;
    unique_ptr<RucksDBSchema> schema_;
    unique_ptr<RucksDBColumnarStorage> storage_;
    
public:
    RucksDBTableRegistry(RocksDBStorage* storage);
    
    void CreateTable(const string& name, const vector<ColumnDefinition>& columns);
    void DropTable(const string& name);
    RucksDBTableStorage* GetTable(const string& name);
    bool TableExists(const string& name);
    
    vector<string> ListTables();
};

// Global registry instance
extern unique_ptr<RucksDBTableRegistry> g_table_registry;

} // namespace duckdb