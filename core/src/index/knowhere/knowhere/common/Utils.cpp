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
#include <iostream>

namespace milvus {
namespace knowhere {

const char* INDEX_FILE_SLICE_SIZE_IN_MEGABYTE = "slice_size";

void
Slice(const std::string& prefix, const BinaryPtr& data_src, const int64_t& slice_len, BinarySet& binarySet,
      milvus::json& ret) {
    if (!data_src)
        return;
    int slice_num = 0;
    for (int64_t i = 0; i < data_src->size; ++slice_num) {
        int64_t ri = std::min(i + slice_len, data_src->size);
        uint8_t* slice_i = (uint8_t*)malloc((size_t)(ri - i));
        memcpy(slice_i, data_src->data.get() + i, (size_t)(ri - i));
        std::shared_ptr<uint8_t[]> slice_i_sp(slice_i, std::default_delete<uint8_t[]>());
        binarySet.Append(prefix + "_" + std::to_string(slice_num), slice_i_sp, ri - i);
        i = ri;
    }
    ret["name"] = prefix;
    ret["slice_num"] = slice_num;
    ret["total_len"] = data_src->size;
}

void
Assemble(BinarySet& binarySet) {
    if (!binarySet.Contains("meta"))
        return;
    milvus::json meta_data = milvus::json::parse(
        std::string((char*)(binarySet.GetByName("meta")->data.get()), binarySet.GetByName("meta")->size));
    for (auto& item : meta_data["meta"]) {
        std::string prefix = item["name"];
        int slice_num = item["slice_num"];
        int64_t total_len = item["total_len"];
        auto p_data = (uint8_t*)malloc(total_len);
        int64_t pos = 0;
        for (auto i = 0; i < slice_num; ++i) {
            auto slice_i_sp = binarySet.Erase(prefix + "_" + std::to_string(i));
            memcpy(p_data + pos, slice_i_sp->data.get(), (size_t)(slice_i_sp->size));
            pos += slice_i_sp->size;
        }
        std::shared_ptr<uint8_t[]> integral_data(p_data, std::default_delete<uint8_t[]>());
        binarySet.Append(prefix, integral_data, total_len);
    }
}

void
Disassemble(const int64_t& slice_size_in_byte, BinarySet& binarySet) {
    milvus::json meta_info;
    std::vector<std::string> slice_key_list;
    for (auto& kv : binarySet.binary_map_) {
        if (kv.second->size > slice_size_in_byte) {
            slice_key_list.push_back(kv.first);
            //            Slice(kv.first, kv.second, binarySet);
        }
    }
    for (auto& key : slice_key_list) {
        milvus::json slice_i;
        Slice(key, binarySet.Erase(key), slice_size_in_byte, binarySet, slice_i);
        meta_info["meta"].emplace_back(slice_i);
    }
    if (slice_key_list.size()) {
        auto meta_str = meta_info.dump();
        std::shared_ptr<uint8_t[]> meta_data(new uint8_t[meta_str.length() + 1], std::default_delete<uint8_t[]>());
        memcpy(meta_data.get(), meta_str.data(), meta_str.length());
        meta_data.get()[meta_str.length()] = 0;
        binarySet.Append("meta", meta_data, meta_str.length() + 1);
    }
}

}  // namespace knowhere
}  // namespace milvus
