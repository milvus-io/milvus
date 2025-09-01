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

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <tuple>

#include "index/json_stats/utils.h"
#include "common/EasyAssert.h"
#include "index/IndexStats.h"

namespace milvus::index {

inline int64_t
EncodeInvertedIndexValue(uint32_t row_id, uint32_t offset) {
    return (static_cast<uint64_t>(row_id) << 32) | offset;
}

inline std::pair<uint32_t, uint32_t>
DecodeInvertedIndexValue(uint64_t value) {
    return std::make_pair(static_cast<uint32_t>(value >> 32),
                          static_cast<uint32_t>(value & 0xFFFFFFFF));
}

class BsonInvertedIndex {
 public:
    BsonInvertedIndex(const std::string& path,
                      int64_t field_id,
                      bool is_load,
                      const storage::FileManagerContext& ctx,
                      int64_t tantivy_index_version);

    ~BsonInvertedIndex();

    void
    AddRecord(const std::string& key, uint32_t row_id, uint32_t offset);

    void
    BuildIndex();

    void
    LoadIndex(const std::vector<std::string>& index_files,
              milvus::proto::common::LoadPriority priority);

    IndexStatsPtr
    UploadIndex();

    void
    TermQuery(const std::string& path,
              const std::function<void(const uint32_t* row_id_array,
                                       const uint32_t* offset_array,
                                       const int64_t array_len)>& visitor);

    void
    TermQueryEach(
        const std::string& path,
        const std::function<void(uint32_t row_id, uint32_t offset)>& each);

    bool
    KeyExists(const std::string& key) {
        auto array = wrapper_->term_query_i64(key);
        return !array.array_.len == 0;
    }

 private:
    std::string path_;
    bool is_load_;
    // json field id that this inverted index belongs to
    int64_t field_id_;
    int64_t tantivy_index_version_;
    // key -> encoded([row_id, offset]) map cache for building index
    std::map<std::string, std::vector<int64_t>> inverted_index_map_;
    // tantivy index wrapper
    std::shared_ptr<TantivyIndexWrapper> wrapper_;
    std::shared_ptr<milvus::storage::DiskFileManagerImpl> disk_file_manager_;
};

}  // namespace milvus::index
