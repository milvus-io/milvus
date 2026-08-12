// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <stdint.h>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "arrow/util/macros.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/resource_c.h"
#include "index/Index.h"
#include "index/IndexInfo.h"
#include "index/ScalarIndex.h"
#include "storage/FileManager.h"
#include "storage/IndexEntryReader.h"

namespace milvus::index {

struct IndexLoadSpec {
    DataType field_type;
    DataType element_type;
    IndexVersion index_version;
    uint64_t index_size_in_bytes;
    const std::map<std::string, std::string>& index_params;
    bool mmap_enable;
    int64_t num_rows;
    int64_t dim;
};

struct IndexFileContext {
    const std::vector<std::string>& index_files;
    const storage::FileManagerContext& file_manager_context;
};

struct ScalarIndexFileInspection {
    IndexType effective_index_type;
    std::optional<storage::EntryStreamLoadInfo> stream_load_info;
};

struct ScalarIndexLoadPlan {
    LoadResourceRequest request;
    std::optional<int64_t> shared_memory_runtime_unit_bytes;
};

class IndexFactory {
 public:
    IndexFactory() = default;
    IndexFactory(const IndexFactory&) = delete;
    IndexFactory
    operator=(const IndexFactory&) = delete;

 public:
    static IndexFactory&
    GetInstance() {
        // thread-safe enough after c++ 11
        static IndexFactory instance;

        return instance;
    }

    static bool
    CanUseIndexRawDataForField(DataType field_type, bool has_raw_data);

    // Whether resource planning needs scalar index file inspection instead of
    // a reusable metadata-only estimate.
    static bool
    RequiresFileContextForLoadResource(const IndexLoadSpec& spec);

    // Metadata-only estimate used by admission and fallback paths. This entry
    // never opens index files.
    LoadResourceRequest
    EstimateIndexLoadResource(const IndexLoadSpec& spec);

    // Inspect scalar index files and normalize the effective index type. This
    // entry performs file I/O but does not calculate a resource request.
    ScalarIndexFileInspection
    InspectScalarIndexFiles(const IndexLoadSpec& spec,
                            const IndexFileContext& files);

    // Build a scalar load plan from metadata and an existing file inspection.
    // This entry performs no file I/O. Packed V3 planning requires persisted
    // stream metadata and never falls back to the metadata-only estimate.
    ScalarIndexLoadPlan
    PlanScalarIndexLoad(const IndexLoadSpec& spec,
                        const ScalarIndexFileInspection& inspection);

    IndexBasePtr
    CreateIndex(const CreateIndexInfo& create_index_info,
                const storage::FileManagerContext& file_manager_context,
                bool use_build_pool = true);

    IndexBasePtr
    CreateVectorIndex(const CreateIndexInfo& create_index_info,
                      const storage::FileManagerContext& file_manager_context,
                      bool use_knowhere_build_pool_ = true);

    // For base types like int, float, double, string, etc
    IndexBasePtr
    CreatePrimitiveScalarIndex(
        DataType data_type,
        const CreateIndexInfo& create_index_info,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    // Create ngram index
    IndexBasePtr
    CreateNgramIndex(DataType data_type,
                     const NgramParams& params,
                     const storage::FileManagerContext& file_manager_context =
                         storage::FileManagerContext());

    // For types like array, struct, union, etc
    IndexBasePtr
    CreateCompositeScalarIndex(
        const CreateIndexInfo& create_index_info,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    // For types like Json, XML, etc
    IndexBasePtr
    CreateComplexScalarIndex(
        IndexType index_type,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateJsonIndex(const CreateIndexInfo& create_index_info,
                    const storage::FileManagerContext& file_manager_context =
                        storage::FileManagerContext());

    IndexBasePtr
    CreateGeometryIndex(
        IndexType index_type,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateNestedIndex(IndexType index_type,
                      int32_t tantivy_index_version,
                      const storage::FileManagerContext& file_manager_context =
                          storage::FileManagerContext());

    IndexBasePtr
    CreateNestedIndexInverted(
        int32_t tantivy_index_version,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateNestedIndexScalarIndexSort(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateNestedIndexBitmap(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateNestedIndexHybrid(
        int32_t tantivy_index_version,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateScalarIndex(const CreateIndexInfo& create_index_info,
                      const storage::FileManagerContext& file_manager_context =
                          storage::FileManagerContext());

    // IndexBasePtr
    // CreateIndex(DataType dtype, const IndexType& index_type);
 private:
    FRIEND_TEST(StringIndexMarisaTest, Reverse);

    LoadResourceRequest
    EstimateVectorIndexLoadResource(const IndexLoadSpec& spec);

    template <typename T>
    ScalarIndexPtr<T>
    CreatePrimitiveScalarIndex(const CreateIndexInfo& create_index_info,
                               const storage::FileManagerContext& file_manager =
                                   storage::FileManagerContext());
};

}  // namespace milvus::index
