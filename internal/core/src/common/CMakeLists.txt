# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

add_source_at_current_directory_recursively()

# Remove mol_c.cpp from SOURCE_FILES - it will be compiled separately with GCC 11
if(DEFINED RDKIT_INCLUDE_DIR)
    # SOURCE_FILES contains relative paths from add_source_at_current_directory_recursively()
    list(REMOVE_ITEM SOURCE_FILES mol_c.cpp)
    message(STATUS "mol_c.cpp removed from milvus_common, will be compiled separately with GCC 11")
endif()

add_library(milvus_common OBJECT ${SOURCE_FILES})

# Build mol_c.cpp separately with GCC 11 (required for RDKit C++20 constexpr destructors)
# GCC 9 does not support constexpr destructors even with -std=c++2a
if(DEFINED RDKIT_INCLUDE_DIR)
    set(MOL_C_SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/mol_c.cpp)
    set(MOL_C_OBJECT ${CMAKE_CURRENT_BINARY_DIR}/mol_c.o)
    
    # Use GCC 11 for mol_c.cpp (RDKit requires C++20 constexpr destructor support)
    if(EXISTS "/usr/bin/g++-11")
        set(MOL_C_COMPILER "/usr/bin/g++-11")
        message(STATUS "mol_c.cpp will be compiled with GCC 11: ${MOL_C_COMPILER}")
    else()
        set(MOL_C_COMPILER ${CMAKE_CXX_COMPILER})
        message(WARNING "GCC 11 not found. mol_c.cpp may fail to compile due to C++20 requirements.")
    endif()
    
    message(STATUS "mol_c.cpp RDKIT_INCLUDE_DIR: ${RDKIT_INCLUDE_DIR}")
    
    # Build include directories list for the custom command
    set(MOL_C_INC_DIRS
        ${RDKIT_INCLUDE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${MILVUS_ENGINE_SRC}
        ${MILVUS_THIRDPARTY_SRC}
    )
    # Add conan include directories
    foreach(inc_dir ${CONAN_INCLUDE_DIRS})
        list(APPEND MOL_C_INC_DIRS ${inc_dir})
    endforeach()
    
    # Convert to -I flags
    set(MOL_C_INC_FLAGS "")
    foreach(inc_dir ${MOL_C_INC_DIRS})
        list(APPEND MOL_C_INC_FLAGS "-I${inc_dir}")
    endforeach()
    
    # Custom command to compile mol_c.cpp with GCC 11
    add_custom_command(
        OUTPUT ${MOL_C_OBJECT}
        COMMAND ${MOL_C_COMPILER}
            -c ${MOL_C_SOURCE}
            -o ${MOL_C_OBJECT}
            -std=c++20
            -fPIC
            -g
            ${MOL_C_INC_FLAGS}
        DEPENDS ${MOL_C_SOURCE} rdkit_ep
        COMMENT "Compiling mol_c.cpp with GCC 11 (C++20)"
    )
    
    # Create a custom target to ensure mol_c.o is built
    add_custom_target(mol_c_build DEPENDS ${MOL_C_OBJECT})
endif()
