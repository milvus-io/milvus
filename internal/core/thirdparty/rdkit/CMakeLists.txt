#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

# RDKit integration for Milvus
# Supports building from local modified source or from Git repository

# Priority 1: Check if RDKIT_SOURCE_DIR environment variable is set (for local modified source)
if(DEFINED ENV{RDKIT_SOURCE_DIR})
    set(RDKIT_SOURCE_DIR "$ENV{RDKIT_SOURCE_DIR}" CACHE PATH "Path to modified RDKit source")
    message(STATUS "Using RDKit from environment variable: ${RDKIT_SOURCE_DIR}")
endif()

# Priority 2: Check if RDKIT_SOURCE_DIR CMake variable is set
if(NOT DEFINED RDKIT_SOURCE_DIR)
    # Default: use the local rdkit in parent directory of milvus
    set(RDKIT_SOURCE_DIR "/home/zilliz/xiejh/rdkit" CACHE PATH "Path to modified RDKit source")
endif()

# Select compiler for RDKit build only (MUST use GCC 11 for C++20 support)
# RDKit requires GCC 11+ for C++20 support (required for constexpr destructors)
# Note: Milvus itself can use GCC 9, but RDKit must use GCC 11
if(EXISTS "/usr/bin/gcc-11" AND EXISTS "/usr/bin/g++-11")
    set(RDKIT_C_COMPILER "/usr/bin/gcc-11")
    set(RDKIT_CXX_COMPILER "/usr/bin/g++-11")
    message(STATUS "RDKit will use GCC 11: C=${RDKIT_C_COMPILER}, CXX=${RDKIT_CXX_COMPILER}")
else()
    message(WARNING "GCC 11 not found at /usr/bin/gcc-11 or /usr/bin/g++-11")
    message(WARNING "RDKit requires GCC 11+ for C++20 support. Falling back to ${CMAKE_C_COMPILER}")
    set(RDKIT_C_COMPILER ${CMAKE_C_COMPILER})
    set(RDKIT_CXX_COMPILER ${CMAKE_CXX_COMPILER})
endif()

# Check if local source exists
if(EXISTS "${RDKIT_SOURCE_DIR}/CMakeLists.txt")
    message(STATUS "Building RDKit from local source: ${RDKIT_SOURCE_DIR}")
    
    # Set RDKit build options
    set(RDKIT_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/rdkit-build")
    
    # RDKit CMake configuration options
    # Minimal build: only SMILES parsing, fingerprints, pickle serialization, substructure matching
    # Required modules:
    #   - SmilesParse: SMILES parsing
    #   - Fingerprints: fingerprint generation (Morgan, RDKit, MACCS)
    #   - SubstructMatch: substructure matching
    #   - PicklerOps (in RDGeneral): pickle serialization
    
    # Ensure RDKit uses GCC 11 (already set above, but verify here)
    if(EXISTS "/usr/bin/gcc-11" AND EXISTS "/usr/bin/g++-11")
        set(RDKIT_C_COMPILER "/usr/bin/gcc-11")
        set(RDKIT_CXX_COMPILER "/usr/bin/g++-11")
        message(STATUS "RDKit build: Using GCC 11 - C=${RDKIT_C_COMPILER}, CXX=${RDKIT_CXX_COMPILER}")
    else()
        message(WARNING "RDKit build: GCC 11 not found, using ${CMAKE_C_COMPILER} (may cause C++20 issues)")
        set(RDKIT_C_COMPILER ${CMAKE_C_COMPILER})
        set(RDKIT_CXX_COMPILER ${CMAKE_CXX_COMPILER})
    endif()
    
    set(RDKIT_CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_C_COMPILER=${RDKIT_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${RDKIT_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD=20
        -DCMAKE_CXX_STANDARD_REQUIRED=ON
        -DCMAKE_CXX_EXTENSIONS=OFF
        -DCMAKE_INSTALL_PREFIX=${RDKIT_BUILD_DIR}
        -DRDK_BUILD_PYTHON_WRAPPERS=OFF
        -DRDK_BUILD_INCHI_SUPPORT=OFF
        -DRDK_BUILD_AVALON_SUPPORT=OFF
        -DRDK_BUILD_CPP_TESTS=OFF
        -DRDK_BUILD_SWIG_WRAPPERS=OFF
        -DRDK_BUILD_DESCRIPTORS3D=OFF
        -DRDK_BUILD_MOLINTERCHANGE_SUPPORT=OFF
        -DRDK_BUILD_FREETYPE_SUPPORT=OFF
        -DRDK_BUILD_COORDGEN_SUPPORT=OFF
        -DRDK_BUILD_CAIRO_SUPPORT=OFF
        -DRDK_BUILD_QT_SUPPORT=OFF
        -DRDK_BUILD_CHEMDRAW_SUPPORT=OFF
        -DRDK_BUILD_MAEPARSER_SUPPORT=OFF
        -DRDK_BUILD_THREADSAFE_SSS=ON
        -DBOOST_ROOT=${CONAN_BOOST_ROOT}
        -DBoost_NO_BOOST_CMAKE=ON
    )
    
    # Enable thread-safe SSS if available (for concurrent access)
    # Uncomment if your modified RDKit supports this option
    # set(RDKIT_CMAKE_ARGS ${RDKIT_CMAKE_ARGS} -DRDK_BUILD_THREADSAFE_SSS=ON)
    
    # Use ExternalProject to build RDKit from local source
    include(ExternalProject)
    
    ExternalProject_Add(
        rdkit_ep
        SOURCE_DIR ${RDKIT_SOURCE_DIR}
        BINARY_DIR ${RDKIT_BUILD_DIR}
        CMAKE_ARGS ${RDKIT_CMAKE_ARGS}
        INSTALL_COMMAND ""
        ${EP_LOG_OPTIONS}
        BUILD_BYPRODUCTS 
            "${RDKIT_BUILD_DIR}/lib/libRDKitGraphMol${CMAKE_SHARED_LIBRARY_SUFFIX}"
            "${RDKIT_BUILD_DIR}/lib/libRDKitSmilesParse${CMAKE_SHARED_LIBRARY_SUFFIX}"
            "${RDKIT_BUILD_DIR}/lib/libRDKitFingerprints${CMAKE_SHARED_LIBRARY_SUFFIX}"
            "${RDKIT_BUILD_DIR}/lib/libRDKitRDGeneral${CMAKE_SHARED_LIBRARY_SUFFIX}"
            "${RDKIT_BUILD_DIR}/lib/libRDKitDataStructs${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
    
    # Set the source directory for includes
    set(RDKIT_SOURCE_INCLUDE_DIR "${RDKIT_SOURCE_DIR}/Code" CACHE INTERNAL "RDKit source include directory")
    
    # Create imported library targets for RDKit components
    # RDGeneral library
    add_library(RDKit::RDKitRDGeneral SHARED IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKitRDGeneral
        PROPERTIES
        IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitRDGeneral${CMAKE_SHARED_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    add_dependencies(RDKit::RDKitRDGeneral rdkit_ep)
    
    # DataStructs library
    add_library(RDKit::RDKitDataStructs SHARED IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKitDataStructs
        PROPERTIES
        IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitDataStructs${CMAKE_SHARED_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    add_dependencies(RDKit::RDKitDataStructs rdkit_ep)
    
    # SmilesParse library
    add_library(RDKit::RDKitSmilesParse SHARED IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKitSmilesParse
        PROPERTIES
        IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitSmilesParse${CMAKE_SHARED_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    add_dependencies(RDKit::RDKitSmilesParse rdkit_ep)
    
    # Main GraphMol library
    add_library(RDKit::RDKitGraphMol SHARED IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKitGraphMol
        PROPERTIES
        IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitGraphMol${CMAKE_SHARED_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    add_dependencies(RDKit::RDKitGraphMol rdkit_ep)
    
    # Fingerprints library
    add_library(RDKit::RDKitFingerprints SHARED IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKitFingerprints
        PROPERTIES
        IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitFingerprints${CMAKE_SHARED_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    add_dependencies(RDKit::RDKitFingerprints rdkit_ep)
    
    # Convenience target that links all RDKit libraries
    add_library(RDKit::RDKit INTERFACE IMPORTED GLOBAL)
    set_target_properties(RDKit::RDKit
        PROPERTIES
        INTERFACE_LINK_LIBRARIES 
            "RDKit::RDKitRDGeneral;RDKit::RDKitDataStructs;RDKit::RDKitGraphMol;RDKit::RDKitSmilesParse;RDKit::RDKitFingerprints"
        INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
    )
    
    # Set global variables for other modules
    set(RDKIT_INCLUDE_DIR "${RDKIT_SOURCE_INCLUDE_DIR}" CACHE INTERNAL "RDKit include directory")
    set(RDKIT_LIB_DIR "${RDKIT_BUILD_DIR}/lib" CACHE INTERNAL "RDKit library directory")
    
    message(STATUS "RDKit build directory: ${RDKIT_BUILD_DIR}")
    message(STATUS "RDKit include directory: ${RDKIT_SOURCE_INCLUDE_DIR}")
    
else()
    # Fallback: Use FetchContent to get RDKit from Git (if local source not found)
    message(STATUS "Local RDKit source not found at ${RDKIT_SOURCE_DIR}")
    message(STATUS "Attempting to fetch RDKit from Git repository...")
    
    include(FetchContent)
    
    # RDKit Git repository and version
    set(RDKIT_VERSION "milvus-modifications" CACHE STRING "RDKit version/branch to use")
    set(RDKIT_GIT_REPOSITORY "https://github.com/862103595/rdkit.git" CACHE STRING "RDKit Git repository")
    
    # Set build directories
    set(RDKIT_FETCH_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/rdkit-src")
    set(RDKIT_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/rdkit-build")
    
    FetchContent_Declare(
        rdkit
        GIT_REPOSITORY ${RDKIT_GIT_REPOSITORY}
        GIT_TAG ${RDKIT_VERSION}
        SOURCE_DIR ${RDKIT_FETCH_SOURCE_DIR}
    )
    
    FetchContent_GetProperties(rdkit)
    if(NOT rdkit_POPULATED)
        FetchContent_Populate(rdkit)
        
        # Ensure RDKit uses GCC 11 (already set above, but verify here)
        if(EXISTS "/usr/bin/gcc-11" AND EXISTS "/usr/bin/g++-11")
            set(RDKIT_C_COMPILER "/usr/bin/gcc-11")
            set(RDKIT_CXX_COMPILER "/usr/bin/g++-11")
            message(STATUS "RDKit build (FetchContent): Using GCC 11 - C=${RDKIT_C_COMPILER}, CXX=${RDKIT_CXX_COMPILER}")
        else()
            message(WARNING "RDKit build (FetchContent): GCC 11 not found, using ${CMAKE_C_COMPILER} (may cause C++20 issues)")
            set(RDKIT_C_COMPILER ${CMAKE_C_COMPILER})
            set(RDKIT_CXX_COMPILER ${CMAKE_CXX_COMPILER})
        endif()
        
        # Configure RDKit build options
        set(RDKIT_CMAKE_ARGS
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_C_COMPILER=${RDKIT_C_COMPILER}
            -DCMAKE_CXX_COMPILER=${RDKIT_CXX_COMPILER}
            -DCMAKE_CXX_STANDARD=20
            -DCMAKE_CXX_STANDARD_REQUIRED=ON
            -DCMAKE_CXX_EXTENSIONS=OFF
            -DCMAKE_INSTALL_PREFIX=${RDKIT_BUILD_DIR}
            -DRDK_BUILD_PYTHON_WRAPPERS=OFF
            -DRDK_BUILD_INCHI_SUPPORT=OFF
            -DRDK_BUILD_AVALON_SUPPORT=OFF
            -DRDK_BUILD_CPP_TESTS=OFF
            -DRDK_BUILD_SWIG_WRAPPERS=OFF
            -DRDK_BUILD_DESCRIPTORS3D=OFF
            -DRDK_BUILD_MOLINTERCHANGE_SUPPORT=OFF
            -DRDK_BUILD_FREETYPE_SUPPORT=OFF
            -DRDK_BUILD_COORDGEN_SUPPORT=OFF
            -DRDK_BUILD_CAIRO_SUPPORT=OFF
            -DRDK_BUILD_QT_SUPPORT=OFF
            -DRDK_BUILD_CHEMDRAW_SUPPORT=OFF
            -DRDK_BUILD_MAEPARSER_SUPPORT=OFF
            -DRDK_BUILD_THREADSAFE_SSS=ON
            -DBOOST_ROOT=${CONAN_BOOST_ROOT}
            -DBoost_NO_BOOST_CMAKE=ON
        )
        
        # Build RDKit using ExternalProject
        include(ExternalProject)
        
        ExternalProject_Add(
            rdkit_ep
            SOURCE_DIR ${rdkit_SOURCE_DIR}
            BINARY_DIR ${RDKIT_BUILD_DIR}
            CMAKE_ARGS ${RDKIT_CMAKE_ARGS}
            INSTALL_COMMAND ""
            ${EP_LOG_OPTIONS}
            BUILD_BYPRODUCTS 
                "${RDKIT_BUILD_DIR}/lib/libRDKitGraphMol${CMAKE_SHARED_LIBRARY_SUFFIX}"
                "${RDKIT_BUILD_DIR}/lib/libRDKitSmilesParse${CMAKE_SHARED_LIBRARY_SUFFIX}"
                "${RDKIT_BUILD_DIR}/lib/libRDKitFingerprints${CMAKE_SHARED_LIBRARY_SUFFIX}"
                "${RDKIT_BUILD_DIR}/lib/libRDKitRDGeneral${CMAKE_SHARED_LIBRARY_SUFFIX}"
                "${RDKIT_BUILD_DIR}/lib/libRDKitDataStructs${CMAKE_SHARED_LIBRARY_SUFFIX}"
        )
        
        # Set the source directory for includes
        set(RDKIT_SOURCE_INCLUDE_DIR "${rdkit_SOURCE_DIR}/Code" CACHE INTERNAL "RDKit source include directory")
        
        # Create imported library targets for RDKit components
        # RDGeneral library
        add_library(RDKit::RDKitRDGeneral SHARED IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKitRDGeneral
            PROPERTIES
            IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitRDGeneral${CMAKE_SHARED_LIBRARY_SUFFIX}"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        add_dependencies(RDKit::RDKitRDGeneral rdkit_ep)
        
        # DataStructs library
        add_library(RDKit::RDKitDataStructs SHARED IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKitDataStructs
            PROPERTIES
            IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitDataStructs${CMAKE_SHARED_LIBRARY_SUFFIX}"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        add_dependencies(RDKit::RDKitDataStructs rdkit_ep)
        
        # SmilesParse library
        add_library(RDKit::RDKitSmilesParse SHARED IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKitSmilesParse
            PROPERTIES
            IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitSmilesParse${CMAKE_SHARED_LIBRARY_SUFFIX}"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        add_dependencies(RDKit::RDKitSmilesParse rdkit_ep)
        
        # Main GraphMol library
        add_library(RDKit::RDKitGraphMol SHARED IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKitGraphMol
            PROPERTIES
            IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitGraphMol${CMAKE_SHARED_LIBRARY_SUFFIX}"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        add_dependencies(RDKit::RDKitGraphMol rdkit_ep)
        
        # Fingerprints library
        add_library(RDKit::RDKitFingerprints SHARED IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKitFingerprints
            PROPERTIES
            IMPORTED_LOCATION "${RDKIT_BUILD_DIR}/lib/libRDKitFingerprints${CMAKE_SHARED_LIBRARY_SUFFIX}"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        add_dependencies(RDKit::RDKitFingerprints rdkit_ep)
        
        # Convenience target that links all RDKit libraries
        add_library(RDKit::RDKit INTERFACE IMPORTED GLOBAL)
        set_target_properties(RDKit::RDKit
            PROPERTIES
            INTERFACE_LINK_LIBRARIES 
                "RDKit::RDKitRDGeneral;RDKit::RDKitDataStructs;RDKit::RDKitGraphMol;RDKit::RDKitSmilesParse;RDKit::RDKitFingerprints"
            INTERFACE_INCLUDE_DIRECTORIES "${RDKIT_SOURCE_INCLUDE_DIR}"
        )
        
        # Set global variables for other modules
        set(RDKIT_INCLUDE_DIR "${RDKIT_SOURCE_INCLUDE_DIR}" CACHE INTERNAL "RDKit include directory")
        set(RDKIT_LIB_DIR "${RDKIT_BUILD_DIR}/lib" CACHE INTERNAL "RDKit library directory")
        
        message(STATUS "RDKit build directory: ${RDKIT_BUILD_DIR}")
        message(STATUS "RDKit include directory: ${RDKIT_SOURCE_INCLUDE_DIR}")
    endif()
endif()

message(STATUS "RDKit integration configured")
message(STATUS "  Include directory: ${RDKIT_INCLUDE_DIR}")
message(STATUS "  Library directory: ${RDKIT_LIB_DIR}")

# Add custom target to copy RDKit libraries to output directory
add_custom_target(copy_rdkit_libs ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_INSTALL_PREFIX}/lib
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${RDKIT_LIB_DIR} ${CMAKE_INSTALL_PREFIX}/lib
    DEPENDS rdkit_ep
    COMMENT "Copying RDKit libraries to output directory"
)
