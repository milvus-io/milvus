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

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/Types.h"
#include "storage/artifact/LoadOptions.h"
#include "common/resource_c.h"
#include "storage/FileManager.h"
#include "cachinglayer/LoadingOverhead.h"

namespace milvus::storage {
class AsyncIndexEntryReader;
}

namespace milvus::index {

bool
CanUseIndexRawDataForField(DataType field_type, bool has_raw_data);

LoadResourceRequest
IndexLoadResource(DataType field_type,
                  DataType element_type,
                  IndexVersion index_version,
                  uint64_t index_size_in_bytes,
                  const std::map<std::string, std::string>& index_params,
                  bool mmap_enable,
                  int64_t num_rows,
                  int64_t dim);

LoadResourceRequest
ScalarIndexLoadResource(DataType field_type,
                        IndexVersion index_version,
                        uint64_t index_size_in_bytes,
                        const std::map<std::string, std::string>& index_params,
                        bool mmap_enable,
                        int64_t num_rows);

struct ScalarIndexLoadResources {
    LoadResourceRequest request;
    std::optional<cachinglayer::LoadingOverheadConfig> overhead;
};

ScalarIndexLoadResources
ScalarIndexFileLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context,
    bool is_index_file = true);

/**
 * @brief Estimate packed scalar loading resources from inspected metadata.
 * @param reader Borrowed directory and metadata; this calculation performs no I/O.
 * @param use_async_load Pinned loading mode used by the caller.
 */
ScalarIndexLoadResources
PackedScalarIndexLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::AsyncIndexEntryReader& reader,
    bool use_async_load);

// Add envelope/decode scratch for the entries actually prepared by async
// vector loading. Native remote-stream engine entries remain uninspected.
LoadResourceRequest
LegacyVectorFileLoadResource(LoadResourceRequest request,
                             bool disk_family,
                             const storage::LoadOptions& options,
                             const std::vector<std::string>& paths,
                             const storage::FileManagerContext& context);

}  // namespace milvus::index
