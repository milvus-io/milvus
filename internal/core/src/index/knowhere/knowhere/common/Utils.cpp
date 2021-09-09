// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "knowhere/common/Utils.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>

namespace milvus {
namespace knowhere {

const char* INDEX_FILE_SLICE_SIZE_IN_MEGABYTE = "SLICE_SIZE";
const char* INDEX_FILE_SLICE_META = "SLICE_META";

static const char* META = "meta";
static const char* NAME = "name";
static const char* SLICE_NUM = "slice_num";
static const char* TOTAL_LEN = "total_len";

void
Slice(const std::string& prefix,
      const BinaryPtr& data_src,
      const int64_t& slice_len,
      BinarySet& binarySet,
      milvus::json& ret) {
    if (!data_src) {
        return;
    }

    int slice_num = 0;
    for (int64_t i = 0; i < data_src->size; ++slice_num) {
        int64_t ri = std::min(i + slice_len, data_src->size);
        auto size = static_cast<size_t>(ri - i);
        auto slice_i = std::shared_ptr<uint8_t[]>(new uint8_t[size]);
        memcpy(slice_i.get(), data_src->data.get() + i, size);
        binarySet.Append(prefix + "_" + std::to_string(slice_num), slice_i, ri - i);
        i = ri;
    }
    ret[NAME] = prefix;
    ret[SLICE_NUM] = slice_num;
    ret[TOTAL_LEN] = data_src->size;
}

void
Assemble(BinarySet& binarySet) {
    auto slice_meta = binarySet.Erase(INDEX_FILE_SLICE_META);
    if (slice_meta == nullptr) {
        return;
    }

    milvus::json meta_data =
        milvus::json::parse(std::string(reinterpret_cast<char*>(slice_meta->data.get()), slice_meta->size));

    for (auto& item : meta_data[META]) {
        std::string prefix = item[NAME];
        int slice_num = item[SLICE_NUM];
        auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
        auto p_data = std::shared_ptr<uint8_t[]>(new uint8_t[total_len]);
        int64_t pos = 0;
        for (auto i = 0; i < slice_num; ++i) {
            auto slice_i_sp = binarySet.Erase(prefix + "_" + std::to_string(i));
            memcpy(p_data.get() + pos, slice_i_sp->data.get(), static_cast<size_t>(slice_i_sp->size));
            pos += slice_i_sp->size;
        }
        binarySet.Append(prefix, p_data, total_len);
    }
}

void
Disassemble(const int64_t& slice_size_in_byte, BinarySet& binarySet) {
    milvus::json meta_info;
    auto slice_meta = binarySet.Erase(INDEX_FILE_SLICE_META);
    if (slice_meta != nullptr) {
        milvus::json last_meta_data =
            milvus::json::parse(std::string(reinterpret_cast<char*>(slice_meta->data.get()), slice_meta->size));
        for (auto& item : last_meta_data[META]) {
            meta_info[META].emplace_back(item);
        }
    }

    std::vector<std::string> slice_key_list;
    for (auto& kv : binarySet.binary_map_) {
        if (kv.second->size > slice_size_in_byte) {
            slice_key_list.push_back(kv.first);
        }
    }
    for (auto& key : slice_key_list) {
        milvus::json slice_i;
        Slice(key, binarySet.Erase(key), slice_size_in_byte, binarySet, slice_i);
        meta_info[META].emplace_back(slice_i);
    }
    if (!slice_key_list.empty()) {
        auto meta_str = meta_info.dump();
        std::shared_ptr<uint8_t[]> meta_data(new uint8_t[meta_str.length() + 1], std::default_delete<uint8_t[]>());
        memcpy(meta_data.get(), meta_str.data(), meta_str.length());
        meta_data.get()[meta_str.length()] = 0;
        binarySet.Append(INDEX_FILE_SLICE_META, meta_data, meta_str.length() + 1);
    }
}

}  // namespace knowhere
}  // namespace milvus
