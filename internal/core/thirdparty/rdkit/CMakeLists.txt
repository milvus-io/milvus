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

# RDKit integration for Milvus — now provided by Conan package
#
# The Conan package "rdkit/2025.09.1@milvus/dev" supplies:
#   - SMILES / MOL parsing  (SmilesParse, GraphMol)
#   - Pickle serialization  (GraphMol/MolPickler)
#   - Molecular fingerprints (Fingerprints – Morgan, MACCS, RDKit)
#   - Substructure matching  (SubstructMatch)
#
# After `conan_basic_setup()` the variables below are available:
#   CONAN_INCLUDE_DIRS_RDKIT   — header search paths
#   CONAN_LIB_DIRS_RDKIT       — library search paths
#   CONAN_LIBS_RDKIT            — library names (e.g. RDKitGraphMol …)

# ---------- sanity check ----------
if(NOT DEFINED CONAN_LIBS_RDKIT)
    message(FATAL_ERROR
        "RDKit Conan package not found. "
        "Make sure 'rdkit/2025.09.1@milvus/dev' is listed in conanfile.py "
        "and `conan install` has been run.")
endif()

# ---------- convenience imported target ----------
# Create RDKit::RDKit interface target so the rest of the build system
# can keep using `target_link_libraries(... RDKit::RDKit)` unchanged.
add_library(RDKit::RDKit INTERFACE IMPORTED GLOBAL)
set_target_properties(RDKit::RDKit PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CONAN_INCLUDE_DIRS_RDKIT}"
    INTERFACE_LINK_DIRECTORIES    "${CONAN_LIB_DIRS_RDKIT}"
    INTERFACE_LINK_LIBRARIES      "${CONAN_LIBS_RDKIT}"
)

# Backward-compat: expose RDKIT_INCLUDE_DIR for mol_c.cpp compilation
set(RDKIT_INCLUDE_DIR "${CONAN_INCLUDE_DIRS_RDKIT}" CACHE INTERNAL
    "RDKit include directories (from Conan)")

message(STATUS "RDKit configured from Conan package")
message(STATUS "  Include dirs : ${CONAN_INCLUDE_DIRS_RDKIT}")
message(STATUS "  Libraries    : ${CONAN_LIBS_RDKIT}")
