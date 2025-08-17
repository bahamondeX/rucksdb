// src/main.cpp
#include <iostream>

// It's good practice to ensure the function from your library is declared.
// If you have include/rucksdb.h with `void rucksdb_init();`, this is already handled.
// If not, you might need: extern void rucksdb_init();

// Add a shutdown function if you implemented one in rucksdb.cpp
// extern void rucksdb_shutdown(); // Uncomment if you added rucksdb_shutdown()

int main() {
    std::cout << "Starting RucksDB Example..." << std::endl;

    // Call the initialization function from your library

    // You can add more logic here to test your RucksDB functionality
    // For instance, if rucksdb_init() prints "Hello from DuckDB!", that's a good start.

    // Call the shutdown function if you implemented one
    // rucksdb_shutdown(); // Uncomment if you added rucksdb_shutdown()

    std::cout << "RucksDB Example Finished." << std::endl;
    return 0; // Indicate successful execution
}
