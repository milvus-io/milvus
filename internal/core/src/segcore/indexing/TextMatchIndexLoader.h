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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IIndex.h"
#include "common/LoadInfo.h"
#include "segcore/indexing/IndexLoadUtils.h"
#include "index/contracts/Registry.h"
#include "index/IndexCacheConfig.h"
#include "common/resource_c.h"
#include "storage/FileManager.h"

namespace milvus::segcore {

struct TextMatchIndexLoadInfo {
    bool enable_mmap;
    int64_t segment_id;
    int64_t field_id;
    std::string analyzer_params;
    int64_t index_size;
    int64_t num_rows;
    std::string
        warmup_policy;  // "disable", "sync", or "async"; empty means use global config
    std::string shard;
};

/** @brief Prepare metadata and construct a sealed index with its own storage cache. */
class TextMatchIndexLoader {
 public:
    TextMatchIndexLoader(
        TextMatchIndexLoadInfo load_info,
        milvus::storage::FileManagerContext file_manager_context,
        milvus::Config config,
        milvus::OpContext* op_ctx = nullptr);

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
    StorageBytes() const;

    index::IndexFamily family_;
    DataType value_type_{DataType::NONE};
    index::ReaderCaps caps_;

    TextMatchIndexLoadInfo load_info_;
    milvus::storage::FileManagerContext file_manager_context_;
    milvus::Config config_;
    std::string key_;
    milvus::cachinglayer::Meta meta_;
    std::optional<LoadResourceRequest> file_load_resource_request_;
    cachinglayer::ResourceUsage shared_overhead_;
    std::shared_ptr<storage::V1RemoteSource> prepared_source_;
};

}  // namespace milvus::segcore
