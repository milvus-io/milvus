cmake_minimum_required(VERSION 3.16)
project(MilvusCMEKCipherPlugin LANGUAGES CXX)

if(NOT MILVUS_SOURCE_DIR)
    message(FATAL_ERROR "MILVUS_SOURCE_DIR is required")
endif()

add_library(CipherPlugin SHARED cipher_plugin.cpp)
target_compile_features(CipherPlugin PRIVATE cxx_std_17)
target_compile_options(CipherPlugin PRIVATE -fPIC)
target_include_directories(CipherPlugin PRIVATE
    "${MILVUS_SOURCE_DIR}/internal/core/src"
)
set_target_properties(CipherPlugin PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "CipherPlugin"
)
