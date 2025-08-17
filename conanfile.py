from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain
from conan.errors import ConanInvalidConfiguration  # Import for compatibility checks
from conan.tools.files import copy  # Import copy for package() method if needed


class RucksDB(ConanFile):
    # Basic package information
    name = "rucksdb"
    version = "0.1.0"
    description = "RucksDB is a database for Rust"
    license = "MIT"
    author = "Oscar Bahamonde"
    url = "https://github.com/bahamondeX/rucksdb"
    topics = ("database", "rust", "rocksdb", "duckdb")

    # Settings and options for the package
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    # Specifies which files to export when this recipe is consumed
    # IMPORTANT: Include CMakeLists.txt here so it's copied to the build folder
    exports_sources = "src/*", "CMakeLists.txt"

    # Generators to create build system integration files for consumers
    generators = "CMakeDeps", "CMakeToolchain"

    # Define direct dependencies of the rucksdb library
    # These are libraries that rucksdb will link against.
    def requirements(self):
        self.requires("duckdb/1.1.3")
        self.requires("rocksdb/10.0.1")

    # Define tools required to build this package (e.g., CMake itself)
    def build_requirements(self):
        # Using a more common CMake version. CMake 4.x is very new and might not be widely available.
        self.tool_requires("cmake/3.28.1")

    # Optional: Method to obtain the source code if it's not exported.
    # For this example, we assume "src/*" is sufficient via exports_sources.
    # def source(self):
    #     self.run(f"git clone {self.url}.git .")

    # NEW: Define the layout of the project
    def layout(self):
        # This will put all build artifacts into a 'build' directory at the root
        # and specifically place Conan's generated files into 'build/conan'
        self.folders.build = "build"
        self.folders.generators = "build/conan"
        self.folders.source = "."  # Source code is in the current directory

    # Method for validating the configuration before building
    def validate(self):
        # Example: Ensure a compatible C++ standard is used for dependencies
        # DuckDB and RocksDB typically require C++17 or newer.
        if self.settings.compiler.cppstd:
            if self.settings.compiler.cppstd not in ["17", "gnu17", "20", "gnu20"]:
                raise ConanInvalidConfiguration(
                    f"{self.ref} requires C++17 or C++20 standard, but "
                    f"'{self.settings.compiler.cppstd}' is used. "
                    "Please update your Conan profile's 'compiler.cppstd' setting."
                )
        else:
            self.output.warning(
                f"Compiler C++ standard not defined in profile. "
                f"Assuming compatibility for {self.ref}. "
                "It's recommended to set 'compiler.cppstd' in your profile."
            )

    # Method for building the package
    def build(self):
        cmake = CMake(self)
        # When building a package, Conan expects the CMakeLists.txt to be in the build folder.
        # The source_folder argument tells CMake where to find the CMakeLists.txt relative to the build folder.
        # Since we exported CMakeLists.txt to the build folder, it's just "."
        # With the new layout, self.source_folder refers to the root of the Conan build folder
        # where CMakeLists.txt is copied.
        cmake.configure(build_script_folder=self.source_folder)
        cmake.build()

    # Method for packaging the built artifacts
    def package(self):
        cmake = CMake(self)
        cmake.install()

        # If you have specific headers or libraries to copy that aren't handled by cmake.install()
        # For example, if your public headers are in a top-level 'include' directory
        # copy(self, "*.h", src=self.source_folder / "include", dst=self.package_folder / "include")

    # Method to define information for consumers of this package
    def package_info(self):
        # Libraries provided by the 'rucksdb' package itself
        self.cpp_info.libs = ["rucksdb"]

        # When you use self.requires(), Conan's generators (like CMakeDeps)
        # automatically handle the linking information for the required packages (duckdb, rocksdb).
        # You do NOT need to list them in self.cpp_info.system_libs.
        # The 'self.cpp_info.requires' attribute is implicitly handled by CMakeDeps
        # for transitive dependencies, but if you were manually creating a custom generator
        # or needed to explicitly declare direct requirements for consumers, you might use:
        # self.cpp_info.requires = ["duckdb::duckdb", "rocksdb::rocksdb"]

        # Include directories for consumers of 'rucksdb'
        self.cpp_info.includedirs = ["include"]
