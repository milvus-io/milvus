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

#include "common/Slice.h"
#include "common/Common.h"
#include "log/Log.h"

namespace milvus {

std::string
GenSlicedFileName(const std::string& prefix, size_t slice_num) {
    return prefix + "_" + std::to_string(slice_num);
}

void
Slice(const std::string& prefix,
      const BianryValuePtr& data_src,
      const size_t data_size,
      const int64_t slice_len,
      BinarySet& binarySet,
      Config& ret) {
    if (!data_src) {
        return;
    }

    int slice_num = 0;
    for (int64_t i = 0; i < data_size; ++slice_num) {
        int64_t ri = std::min(i + slice_len, (int64_t)data_size);
        auto size = static_cast<size_t>(ri - i);
        auto slice_i = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
        memcpy(slice_i.get(), data_src + i, size);
        binarySet.Append(GenSlicedFileName(prefix, slice_num),
                         Binary(std::move(slice_i), ri - i));
        i = ri;
    }
    ret[NAME] = prefix;
    ret[SLICE_NUM] = slice_num;
    ret[TOTAL_LEN] = data_size;
}

void
Assemble(BinarySet& binarySet) {
    auto slice_meta = EraseSliceMeta(binarySet);
    if (slice_meta.GetSeq() == nullptr) {
        return;
    }

    Config meta_data = Config::parse(std::string(
        reinterpret_cast<char*>(slice_meta.GetSeq()), slice_meta.GetSize()));

    for (auto& item : meta_data[META]) {
        std::string prefix = item[NAME];
        int slice_num = item[SLICE_NUM];
        auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
        auto p_data = std::unique_ptr<uint8_t[]>(new uint8_t[total_len]);
        int64_t pos = 0;
        for (auto i = 0; i < slice_num; ++i) {
            auto slice_i_sp = binarySet.Erase(GenSlicedFileName(prefix, i));
            memcpy(p_data.get() + pos,
                   slice_i_sp.GetSeq(),
                   static_cast<size_t>(slice_i_sp.GetSize()));
            pos += slice_i_sp.GetSize();
        }
        binarySet.Append(prefix, Binary(std::move(p_data), total_len));
    }
}

void
Disassemble(BinarySet& binarySet) {
    Config meta_info;
    auto slice_meta = EraseSliceMeta(binarySet);
    if (slice_meta.GetSeq() != nullptr) {
        Config last_meta_data = Config::parse(
            std::string(reinterpret_cast<char*>(slice_meta.GetSeq()),
                        slice_meta.GetSize()));
        for (auto& item : last_meta_data[META]) {
            meta_info[META].emplace_back(item);
        }
    }

    auto all_keys_list = binarySet.GetBinarySetAllKeys();
    std::vector<std::string> slice_key_list;
    for (auto& key : all_keys_list) {
        if (binarySet.GetBinarySizeByName(key) > FILE_SLICE_SIZE) {
            slice_key_list.push_back(key);
        }
    }
    for (auto& key : slice_key_list) {
        Config slice_i;
        auto binary = binarySet.Erase(key);
        auto binary_ptr = binary.GetSeq();
        auto binary_size = binary.GetSize();
        Slice(
            key, binary_ptr, binary_size, FILE_SLICE_SIZE, binarySet, slice_i);
        meta_info[META].emplace_back(slice_i);
    }
    if (!slice_key_list.empty()) {
        AppendSliceMeta(binarySet, meta_info);
    }
}

void
AppendSliceMeta(BinarySet& binarySet, const Config& meta_info) {
    auto meta_str = meta_info.dump();
    auto meta_len = meta_str.length();
    std::unique_ptr<uint8_t[]> meta_data(new uint8_t[meta_len + 1]);
    memcpy(meta_data.get(), meta_str.data(), meta_len);
    meta_data[meta_len] = 0;
    binarySet.Append(INDEX_FILE_SLICE_META,
                     Binary(std::move(meta_data), meta_len + 1));
}

Binary
EraseSliceMeta(BinarySet& binarySet) {
    return binarySet.Erase(INDEX_FILE_SLICE_META);
}

}  // namespace milvus
