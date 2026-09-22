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
# Test helpers have ordinary .cpp names but belong only to index_tests.
list(FILTER SOURCE_FILES EXCLUDE REGEX "^test_utils/")

# These files intentionally define matching translation-unit-local helpers
# (e.g. kEmptyEmbListOffsets). Keep them separate when the target uses unity
# builds. NOTE: milvus_index is not in src/CMakeLists.txt's unity target list
# today (the per-family layout collides far beyond these four files -- see the
# comment there), so these properties are currently inert; they stay as the
# already-known subset for whenever the target is re-enabled.
set_source_files_properties(
    vector/VectorDiskBuilder.cpp
    vector/VectorDiskLoader.cpp
    vector/VectorMemArtifact.cpp
    vector/VectorMemLoader.cpp
    PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)

add_library(milvus_index OBJECT ${SOURCE_FILES})
# libsais (PUBLIC) propagates its include dir so the vendored FM-index sources
# under fmindex/ can #include "libsais.h"; milvus_core links the objects (below).
target_link_libraries(milvus_index PUBLIC milvus_conan_deps libsais)
