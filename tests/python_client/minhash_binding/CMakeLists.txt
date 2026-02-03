cmake_minimum_required(VERSION 3.15)
project(milvus_minhash)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find pybind11
find_package(pybind11 REQUIRED)

# Find OpenSSL
find_package(OpenSSL REQUIRED)

# Find xxhash (via pkg-config or conan)
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(XXHASH libxxhash)
endif()

if(NOT XXHASH_FOUND)
    # Try to find xxhash manually
    find_path(XXHASH_INCLUDE_DIRS xxhash.h)
    find_library(XXHASH_LIBRARIES xxhash)
endif()

# Create the Python module
pybind11_add_module(milvus_minhash minhash_binding.cpp)

target_include_directories(milvus_minhash PRIVATE
    ${OPENSSL_INCLUDE_DIR}
    ${XXHASH_INCLUDE_DIRS}
)

target_link_libraries(milvus_minhash PRIVATE
    OpenSSL::Crypto
    ${XXHASH_LIBRARIES}
)
