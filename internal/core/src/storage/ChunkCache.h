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
#include <future>
#include <unordered_map>
#include "common/FieldMeta.h"
#include "storage/MmapChunkManager.h"
#include "mmap/ChunkedColumn.h"

namespace milvus::storage {

extern std::map<std::string, int> ReadAheadPolicy_Map;

class ChunkCache {
 public:
    explicit ChunkCache(std::string& path_prefix,
                        const std::string& read_ahead_policy,
                        ChunkManagerPtr cm,
                        MmapChunkManagerPtr mcm)
        : path_prefix_(path_prefix), cm_(cm), mcm_(mcm) {
        auto iter = ReadAheadPolicy_Map.find(read_ahead_policy);
        AssertInfo(iter != ReadAheadPolicy_Map.end(),
                   "unrecognized read ahead policy: {}, "
                   "should be one of `normal, random, sequential, "
                   "willneed, dontneed`",
                   read_ahead_policy);
        read_ahead_policy_ = iter->second;
        LOG_INFO("Init ChunkCache with read_ahead_policy: {}",
                 read_ahead_policy);
    }

    ~ChunkCache() = default;

 public:
    std::shared_ptr<ColumnBase>
    Read(const std::string& filepath,
         const FieldMeta& field_meta,
         bool mmap_enabled,
         bool mmap_rss_not_need = false);

    std::shared_ptr<ColumnBase>
    Read(const std::string& filepath,
         const MmapChunkDescriptorPtr& descriptor,
         const FieldMeta& field_meta,
         bool mmap_enabled,
         bool mmap_rss_not_need = false);

    void
    Remove(const std::string& filepath);

    void
    Prefetch(const std::string& filepath);

 private:
    std::string
    CachePath(const std::string& filepath);

    std::shared_ptr<ColumnBase>
    ConvertToColumn(const FieldDataPtr& field_data,
                    const MmapChunkDescriptorPtr& descriptor,
                    const FieldMeta& field_meta,
                    bool mmap_enabled);

 private:
    using ColumnTable = std::unordered_map<
        std::string,
        std::pair<std::promise<std::shared_ptr<ColumnBase>>,
                  std::shared_future<std::shared_ptr<ColumnBase>>>>;

 private:
    mutable std::shared_mutex mutex_;
    int read_ahead_policy_;
    ChunkManagerPtr cm_;
    MmapChunkManagerPtr mcm_;
    ColumnTable columns_;

    std::string path_prefix_;
};

using ChunkCachePtr = std::shared_ptr<milvus::storage::ChunkCache>;

}  // namespace milvus::storage
