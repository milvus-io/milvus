// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/Tracer.h"
#include "common/resource_c.h"
#include "knowhere/index/index_factory.h"
#include "segcore/indexing/IndexLoadUtils.h"
#include "index/contracts/Registry.h"
#include "index/IndexCacheConfig.h"
#include "storage/FileManager.h"
#include "storage/IndexEntryReader.h"

namespace milvus::segcore {
struct LoadIndexInfo;
}

namespace milvus::segcore {

/** @brief Prepare metadata and construct a sealed index with its own storage cache. */
class SealedIndexLoader {
 public:
    // op_ctx is borrowed only for constructor metadata I/O; each Load binds its own context.
    SealedIndexLoader(const milvus::segcore::LoadIndexInfo* load_index_info,
                      milvus::tracer::TraceContext ctx,
                      milvus::storage::FileManagerContext file_manager_context,
                      Config config,
                      milvus::OpContext* op_ctx = nullptr);
    // Creates an Index whose concrete implementation owns its storage cache.
    index::IndexPtr
    CreateIndex(milvus::OpContext* ctx);

    const index::IndexFamily&
    Family() const noexcept {
        return family_;
    }
    DataType
    ValueType() const noexcept {
        return value_type_;
    }
    const index::ReaderCaps&
    Caps() const noexcept {
        return caps_;
    }

    cachinglayer::LoadingResourceUsage
    EstimatedResources() const;
    const cachinglayer::Meta&
    CacheMetadata() const noexcept {
        return meta_;
    }

 private:
    int64_t
    StorageBytes() const {
        // if index data supports lazy load internally, cell storage size becomes 0
        // currently only vector index is possible to support lazy load
        if (IsVectorDataType(index_load_info_.field_type) && lazy_load_) {
            return 0;
        }
        constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
        return std::max(index_load_info_.index_size, MIN_STORAGE_BYTES);
    }

    index::IndexFamily family_;
    DataType value_type_{DataType::NONE};
    index::ReaderCaps caps_;

    struct IndexLoadInfo {
        bool enable_mmap;
        std::string mmap_dir_path;
        DataType field_type;
        DataType element_type;
        std::map<std::string, std::string> index_params;
        int64_t index_size;
        int64_t index_engine_version;
        std::string index_id;
        std::string segment_id;
        std::string field_id;
        int64_t num_rows;
        int64_t dim;
        std::vector<std::string> index_files;
        std::string
            warmup_policy;  // "disable", "sync", or "async"; empty means use global config
        std::optional<LoadResourceRequest> load_resource_request;
    };

    IndexType index_type_;
    milvus::tracer::TraceContext ctx_;
    milvus::storage::FileManagerContext file_manager_context_;
    Config config_;
    milvus::index::IndexFamily source_family_;
    std::string index_key_;
    IndexLoadInfo index_load_info_;
    LoadResourceRequest load_resource_request_{};
    cachinglayer::ResourceUsage shared_overhead_;
    // Consumed by the first index creation attempt.
    std::shared_ptr<storage::V1RemoteSource> prepared_legacy_source_;
    bool lazy_load_{false};
    milvus::cachinglayer::Meta meta_;
};
}  // namespace milvus::segcore
