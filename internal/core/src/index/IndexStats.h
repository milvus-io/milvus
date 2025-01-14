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
#include <vector>
#include "common/protobuf_utils.h"
#include "pb/cgo_msg.pb.h"

namespace milvus::index {

class SerializedIndexFileInfo {
 public:
    SerializedIndexFileInfo(const std::string& file_name, int64_t file_size)
        : file_name(file_name), file_size(file_size) {
    }

    std::string file_name;
    int64_t file_size;
};

class IndexStats;

using IndexStatsPtr = std::unique_ptr<IndexStats>;

class IndexStats {
 public:
    static IndexStatsPtr
    NewFromSizeMap(int64_t mem_size,
                   std::map<std::string, int64_t>& index_size_map);

    // Create a new IndexStats instance.
    static IndexStatsPtr
    New(int64_t mem_size,
        std::vector<SerializedIndexFileInfo>&& serialized_index_infos);

    IndexStats(const IndexStats&) = delete;

    IndexStats(IndexStats&&) = delete;

    IndexStats&
    operator=(const IndexStats&) = delete;

    IndexStats&
    operator=(IndexStats&&) = delete;

    // Append a new serialized index file info into the result.
    void
    AppendSerializedIndexFileInfo(SerializedIndexFileInfo&& info);

    // Serialize the result into the target proto layout.
    void
    SerializeAt(milvus::ProtoLayout* layout);

    std::vector<std::string>
    GetIndexFiles() const;

    int64_t
    GetMemSize() const;

    int64_t
    GetSerializedSize() const;

 private:
    IndexStats(int64_t mem_size,
               std::vector<SerializedIndexFileInfo>&& serialized_index_infos);

    int64_t mem_size_;
    std::vector<SerializedIndexFileInfo> serialized_index_infos_;
};
}  // namespace milvus::index
