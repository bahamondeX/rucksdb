// src/main.cpp
#include <duckdb.hpp>
#include <iostream> // For std::cout, std::cerr, std::endl
#include <string>
#include <stdlib.h>

// It's good practice to ensure the function from your library is declared.
// If you have include/rucksdb.h with `void rucksdb_init();`, this is already handled.
// If not, you might need: extern void rucksdb_init();

// Add a shutdown function if you implemented one in rucksdb.cpp
// extern void rucksdb_shutdown(); // Uncomment if you added rucksdb_shutdown()

int main() {
    auto query = "INSTALL vss;LOAD vss;CREATE TABLE my_vector_table (vec FLOAT[3]);INSERT INTO my_vector_table SELECT array_value(a, b, c) FROM range(1, 10) ra(a), range(1, 10) rb(b), range(1, 10) rc(c);CREATE INDEX my_hnsw_index ON my_vector_table USING HNSW (vec);";
    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);
    con.Query(query);
    auto result = con.Query("SELECT * FROM my_vector_table ORDER BY array_distance(vec, [1, 2, 3]::FLOAT[3]) LIMIT 3;");
    if (result->HasError()) {
        std::cerr << result->GetError() << std::endl;
    } else {
        std::cout << result->ToString() << std::endl;
    }
    return 0;
}
