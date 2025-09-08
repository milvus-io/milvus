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
#include <string>
#include <boost/filesystem.hpp>
#include <optional>
#include <cstddef>
#include <vector>
#include <folly/SharedMutex.h>
#include "index/Index.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "common/PrimaryIndex.h"

namespace milvus::index {

template <typename T>
struct SegmentData {
    uint64_t segment_id;
    std::vector<T> keys;

    SegmentData(uint64_t id) : segment_id(id) {
    }

    void
    add_key(const T& key) {
        keys.push_back(key);
    }
};

template <typename T>
class PrimaryIndex {
 public:
    explicit PrimaryIndex(const storage::FileManagerContext& ctx, bool is_load);

    IndexStatsPtr
    Upload(const Config& config = {});

    void
    Load(const Config& config);

    void
    BuildWithPrimaryKeys();

    void
    BuildWithPrimaryKeys(const std::vector<SegmentData<T>>& segments);

    void
    Build(const Config& config);

    int64_t
    query(T primary_key);

    void
    reset_segment_id(int64_t to_segment_id, int64_t from_segment_id);

    std::vector<int64_t>
    get_segment_list();

 private:
    std::string path_;
    std::shared_ptr<storage::MemFileManagerImpl> mem_file_manager_;
    std::shared_ptr<storage::DiskFileManagerImpl> disk_file_manager_;
    std::unique_ptr<primaryIndex::PrimaryIndex<T>> primary_index_;
    bool is_built_ = false;
};

}  // namespace milvus::index