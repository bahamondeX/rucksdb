#include "../include/RucksDBExtension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/exception.hpp"
#include <chrono>
#include <sstream>

namespace duckdb {

// Global registry instance
unique_ptr<RucksDBTableRegistry> g_table_registry;

// Helper functions for SQL interface
static void CreateRocksDBTableFunction(DataChunk& args, ExpressionState& state, Vector& result);
static void DropRocksDBTableFunction(DataChunk& args, ExpressionState& state, Vector& result);

// Extension implementation
void RucksDBExtension::Load(DuckDB &db) {
    // Initialize global registry if not already done
    if (!g_table_registry && g_rocksdb_storage) {
        g_table_registry = make_unique<RucksDBTableRegistry>(g_rocksdb_storage.get());
    }
    
    // Register table functions
    RocksDBTableFunction::RegisterFunction(*db.instance);
    
    // Register custom scalar functions
    ScalarFunction create_rocksdb_table("create_rocksdb_table", 
                                       {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                                       LogicalType::BOOLEAN,
                                       CreateRocksDBTableFunction);
    ExtensionUtil::RegisterFunction(*db.instance, create_rocksdb_table);
    
    ScalarFunction drop_rocksdb_table("drop_rocksdb_table", 
                                     {LogicalType::VARCHAR}, 
                                     LogicalType::BOOLEAN,
                                     DropRocksDBTableFunction);
    ExtensionUtil::RegisterFunction(*db.instance, drop_rocksdb_table);
}

// Schema implementation
void RucksDBSchema::CreateTable(const string& table_name, const vector<ColumnDefinition>& columns) {
    // Simple serialization without DuckDB's serializer classes
    string schema_data = std::to_string(columns.size()) + "|";
    
    for (const auto& col : columns) {
        schema_data += col.Name() + ":" + col.Type().ToString() + "|";
    }
    
    string key = string(SCHEMA_PREFIX) + table_name;
    storage_->WriteData(key, schema_data);
    
    // Initialize table metadata
    StoreTableMetadata(table_name, 0);
}

void RucksDBSchema::DropTable(const string& table_name) {
    string schema_key = string(SCHEMA_PREFIX) + table_name;
    storage_->DeleteData(schema_key);
    
    string meta_key = string(TABLE_META_PREFIX) + table_name;
    storage_->DeleteData(meta_key);
    
    // Delete all table data
    string table_prefix = "data_" + table_name + "_";
    storage_->IteratePrefix(table_prefix, [this](const string& key, const string& value) {
        storage_->DeleteData(key);
        return true;
    });
}

vector<ColumnDefinition> RucksDBSchema::GetTableSchema(const string& table_name) {
    string key = string(SCHEMA_PREFIX) + table_name;
    string value;
    
    if (!storage_->ReadData(key, value)) {
        throw std::runtime_error("Table '" + table_name + "' does not exist");
    }
    
    // Simple deserialization
    vector<ColumnDefinition> columns;
    std::istringstream ss(value);
    string token;
    
    // Get column count
    std::getline(ss, token, '|');
    size_t column_count = std::stoull(token);
    
    // Parse each column
    for (size_t i = 0; i < column_count; i++) {
        std::getline(ss, token, '|');
        if (token.empty()) break;
        
        size_t colon_pos = token.find(':');
        if (colon_pos != string::npos) {
            string name = token.substr(0, colon_pos);
            string type_str = token.substr(colon_pos + 1);
            
            // Basic type mapping
            LogicalType type;
            if (type_str == "INTEGER") {
                type = LogicalType::INTEGER;
            } else if (type_str == "VARCHAR") {
                type = LogicalType::VARCHAR;
            } else if (type_str == "FLOAT") {
                type = LogicalType::FLOAT;
            } else {
                type = LogicalType::VARCHAR; // Default fallback
            }
            
            columns.emplace_back(name, type);
        }
    }
    
    return columns;
}

bool RucksDBSchema::TableExists(const string& table_name) {
    string key = string(SCHEMA_PREFIX) + table_name;
    string value;
    return storage_->ReadData(key, value);
}

void RucksDBSchema::StoreTableMetadata(const string& table_name, idx_t row_count) {
    string key = string(TABLE_META_PREFIX) + table_name;
    string value = to_string(row_count);
    storage_->WriteData(key, value);
}

idx_t RucksDBSchema::LoadTableRowCount(const string& table_name) {
    string key = string(TABLE_META_PREFIX) + table_name;
    string value;
    if (storage_->ReadData(key, value)) {
        return std::stoull(value);
    }
    return 0;
}

// Columnar storage implementation
string RucksDBColumnarStorage::GetRowKey(const string& table_name, idx_t row_id) {
    return "data_" + table_name + "_row_" + to_string(row_id);
}

void RucksDBColumnarStorage::WriteRow(const string& table_name, idx_t row_id, 
                                    const DataChunk& chunk, idx_t chunk_row) {
    // Simple serialization without DuckDB serializers
    string row_data = std::to_string(chunk.ColumnCount()) + "|";
    
    for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
        auto value = chunk.data[col_idx].GetValue(chunk_row);
        
        // Simple value serialization
        if (value.IsNull()) {
            row_data += "NULL|";
        } else {
            switch (value.type().id()) {
                case LogicalTypeId::INTEGER:
                    row_data += "INT:" + std::to_string(value.GetValue<int32_t>()) + "|";
                    break;
                case LogicalTypeId::FLOAT:
                    row_data += "FLOAT:" + std::to_string(value.GetValue<float>()) + "|";
                    break;
                case LogicalTypeId::VARCHAR:
                    row_data += "VARCHAR:" + value.GetValue<string>() + "|";
                    break;
                default:
                    row_data += "VARCHAR:" + value.ToString() + "|";
                    break;
            }
        }
    }
    
    string key = GetRowKey(table_name, row_id);
    storage_->WriteData(key, row_data);
}

bool RucksDBColumnarStorage::ReadRow(const string& table_name, idx_t row_id, 
                                   DataChunk& result, idx_t result_row,
                                   const vector<column_t>& column_ids) {
    string key = GetRowKey(table_name, row_id);
    string value;
    
    if (!storage_->ReadData(key, value)) {
        return false;
    }
    
    // Simple deserialization
    std::istringstream ss(value);
    string token;
    
    // Get column count
    std::getline(ss, token, '|');
    size_t column_count = std::stoull(token);
    
    // Parse values
    vector<Value> values;
    for (size_t i = 0; i < column_count; i++) {
        std::getline(ss, token, '|');
        if (token.empty()) break;
        
        if (token == "NULL") {
            values.push_back(Value());
        } else {
            size_t colon_pos = token.find(':');
            if (colon_pos != string::npos) {
                string type_str = token.substr(0, colon_pos);
                string value_str = token.substr(colon_pos + 1);
                
                if (type_str == "INT") {
                    values.push_back(Value::INTEGER(std::stoi(value_str)));
                } else if (type_str == "FLOAT") {
                    values.push_back(Value::FLOAT(std::stof(value_str)));
                } else if (type_str == "VARCHAR") {
                    values.push_back(Value(value_str));
                } else {
                    values.push_back(Value(value_str));
                }
            }
        }
    }
    
    // Set values for requested columns
    for (idx_t i = 0; i < column_ids.size(); i++) {
        column_t col_id = column_ids[i];
        if (col_id < values.size()) {
            result.data[i].SetValue(result_row, values[col_id]);
        }
    }
    
    return true;
}

void RucksDBColumnarStorage::DeleteRow(const string& table_name, idx_t row_id) {
    string key = GetRowKey(table_name, row_id);
    storage_->DeleteData(key);
}

void RucksDBColumnarStorage::WriteChunk(const string& table_name, idx_t start_row, 
                                      const DataChunk& chunk) {
    for (idx_t i = 0; i < chunk.size(); i++) {
        WriteRow(table_name, start_row + i, chunk, i);
    }
}

idx_t RucksDBColumnarStorage::ScanRows(const string& table_name, idx_t start_row, idx_t max_count,
                                     DataChunk& result, const vector<column_t>& column_ids) {
    idx_t rows_read = 0;
    result.Reset();
    
    for (idx_t row_id = start_row; row_id < start_row + max_count && rows_read < STANDARD_VECTOR_SIZE; row_id++) {
        if (ReadRow(table_name, row_id, result, rows_read, column_ids)) {
            rows_read++;
        }
    }
    
    result.SetCardinality(rows_read);
    return rows_read;
}

// Table storage implementation
RucksDBTableStorage::RucksDBTableStorage(const string& table_name, RucksDBSchema* schema, 
                                       RucksDBColumnarStorage* storage)
    : table_name_(table_name), schema_(schema), storage_(storage), row_count_(0) {
}

void RucksDBTableStorage::Initialize(const vector<ColumnDefinition>& columns) {
    columns_ = columns;
    row_count_ = schema_->LoadTableRowCount(table_name_);
}

void RucksDBTableStorage::Append(DataChunk& chunk) {
    storage_->WriteChunk(table_name_, row_count_, chunk);
    row_count_ += chunk.size();
    schema_->StoreTableMetadata(table_name_, row_count_);
}

void RucksDBTableStorage::InitializeScan(TableScanState& state, const vector<column_t>& column_ids) {
    auto& scan_state = (RucksDBScanState&)state;
    scan_state.current_row = 0;
    scan_state.total_rows = row_count_;
    scan_state.table_name = table_name_;
    scan_state.column_ids = column_ids;
    scan_state.finished = false;
}

void RucksDBTableStorage::Scan(DataChunk& result, TableScanState& state, 
                             const vector<column_t>& column_ids) {
    auto& scan_state = (RucksDBScanState&)state;
    
    if (scan_state.finished || scan_state.current_row >= scan_state.total_rows) {
        return;
    }
    
    idx_t rows_to_read = std::min((idx_t)STANDARD_VECTOR_SIZE, 
                                 scan_state.total_rows - scan_state.current_row);
    
    idx_t rows_read = storage_->ScanRows(table_name_, scan_state.current_row, rows_to_read,
                                       result, column_ids);
    
    scan_state.current_row += rows_read;
    
    if (scan_state.current_row >= scan_state.total_rows || rows_read == 0) {
        scan_state.finished = true;
    }
}

// Table function implementation
void RocksDBTableFunction::RegisterFunction(DatabaseInstance& db) {
    TableFunction rocksdb_scan("rocksdb_scan", {LogicalType::VARCHAR}, Bind, InitGlobal, InitLocal);
    rocksdb_scan.function = Execute;
    ExtensionUtil::RegisterFunction(db, rocksdb_scan);
}

unique_ptr<FunctionData> RocksDBTableFunction::Bind(ClientContext& context, 
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types, 
                                                   vector<string>& names) {
    auto table_name = input.inputs[0].GetValue<string>();
    
    if (!g_table_registry || !g_table_registry->TableExists(table_name)) {
        throw std::runtime_error("RocksDB table '" + table_name + "' does not exist");
    }
    
    auto table_storage = g_table_registry->GetTable(table_name);
    auto columns = table_storage->GetColumns();
    
    for (const auto& col : columns) {
        return_types.push_back(col.Type());
        names.push_back(col.Name());
    }
    
    auto bind_data = make_unique<RocksDBBindData>();
    bind_data->table_name = table_name;
    bind_data->types = return_types;
    bind_data->names = names;
    bind_data->table_storage = table_storage;
    
    return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> RocksDBTableFunction::InitGlobal(ClientContext& context,
                                                                     TableFunctionInitInput& input) {
    auto& bind_data = (RocksDBBindData&)*input.bind_data;
    
    auto global_state = make_unique<RocksDBGlobalState>();
    global_state->table_name = bind_data.table_name;
    global_state->total_rows = bind_data.table_storage->GetRowCount();
    
    return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> RocksDBTableFunction::InitLocal(ExecutionContext& context,
                                                                   TableFunctionInitInput& input,
                                                                   GlobalTableFunctionState* global_state) {
    return make_unique<RucksDBScanState>();
}

void RocksDBTableFunction::Execute(ClientContext& context, TableFunctionInput& data, 
                                 DataChunk& output) {
    auto& bind_data = (RocksDBBindData&)*data.bind_data;
    auto& local_state = (RucksDBScanState&)*data.local_state;
    
    // Initialize column ids for all columns
    if (local_state.column_ids.empty()) {
        for (idx_t i = 0; i < bind_data.types.size(); i++) {
            local_state.column_ids.push_back(i);
        }
        bind_data.table_storage->InitializeScan(local_state, local_state.column_ids);
    }
    
    bind_data.table_storage->Scan(output, local_state, local_state.column_ids);
}

// Table registry implementation
RucksDBTableRegistry::RucksDBTableRegistry(RocksDBStorage* storage) {
    schema_ = make_unique<RucksDBSchema>(storage);
    storage_ = make_unique<RucksDBColumnarStorage>(storage);
}

void RucksDBTableRegistry::CreateTable(const string& name, const vector<ColumnDefinition>& columns) {
    if (TableExists(name)) {
        throw std::runtime_error("Table '" + name + "' already exists");
    }
    
    schema_->CreateTable(name, columns);
    
    auto table_storage = make_unique<RucksDBTableStorage>(name, schema_.get(), storage_.get());
    table_storage->Initialize(columns);
    
    tables_[name] = std::move(table_storage);
}

void RucksDBTableRegistry::DropTable(const string& name) {
    if (!TableExists(name)) {
        throw std::runtime_error("Table '" + name + "' does not exist");
    }
    
    schema_->DropTable(name);
    tables_.erase(name);
}

RucksDBTableStorage* RucksDBTableRegistry::GetTable(const string& name) {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second.get();
    }
    
    // Try to load from storage
    if (schema_->TableExists(name)) {
        auto columns = schema_->GetTableSchema(name);
        auto table_storage = make_unique<RucksDBTableStorage>(name, schema_.get(), storage_.get());
        table_storage->Initialize(columns);
        
        auto* ptr = table_storage.get();
        tables_[name] = std::move(table_storage);
        return ptr;
    }
    
    return nullptr;
}

bool RucksDBTableRegistry::TableExists(const string& name) {
    return tables_.find(name) != tables_.end() || schema_->TableExists(name);
}

vector<string> RucksDBTableRegistry::ListTables() {
    vector<string> table_names;
    for (const auto& pair : tables_) {
        table_names.push_back(pair.first);
    }
    return table_names;
}

// Helper functions for SQL interface
static void CreateRocksDBTableFunction(DataChunk& args, ExpressionState& state, Vector& result) {
    auto table_name = args.data[0].GetValue(0).GetValue<string>();
    auto schema_sql = args.data[1].GetValue(0).GetValue<string>();
    
    // Parse simple schema: "col1 TYPE, col2 TYPE, ..."
    vector<ColumnDefinition> columns;
    // TODO: Implement proper SQL parsing
    // For now, assume simple format
    
    bool success = false;
    if (g_table_registry) {
        try {
            g_table_registry->CreateTable(table_name, columns);
            success = true;
        } catch (...) {
            success = false;
        }
    }
    
    result.SetValue(0, Value::BOOLEAN(success));
}

static void DropRocksDBTableFunction(DataChunk& args, ExpressionState& state, Vector& result) {
    auto table_name = args.data[0].GetValue(0).GetValue<string>();
    
    bool success = false;
    if (g_table_registry) {
        try {
            g_table_registry->DropTable(table_name);
            success = true;
        } catch (...) {
            success = false;
        }
    }
    
    result.SetValue(0, Value::BOOLEAN(success));
}

} // namespace duckdb