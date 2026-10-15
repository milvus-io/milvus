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

#include "common/FastMem.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Slice.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "storage/Util.h"

namespace milvus::index {

std::map<std::string, IndexDataCodec>
CompactIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas) {
    std::map<std::string, IndexDataCodec> index_file_slices;
    std::unordered_set<std::string> compacted_files;
    if (index_datas.find(INDEX_FILE_SLICE_META) != index_datas.end()) {
        auto slice_meta = std::move(index_datas.at(INDEX_FILE_SLICE_META));
        Config meta_data = Config::parse(std::string(
            reinterpret_cast<const char*>(slice_meta->PayloadData()),
            slice_meta->PayloadSize()));
        compacted_files.insert(INDEX_FILE_SLICE_META);
        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
            size_t data_len = 0;
            index_file_slices.insert({prefix, IndexDataCodec{}});
            auto& index_data_codec = index_file_slices.at(prefix);
            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                if (!(index_datas.find(file_name) != index_datas.end())) {
                    ThrowInfo(ErrorCode::DataFormatBroken,
                              "lost index slice data");
                }
                index_data_codec.codecs_.push_back(
                    std::move(index_datas.at(file_name)));
                compacted_files.insert(file_name);
                data_len += index_data_codec.codecs_.back()->PayloadSize();
            }
            if (!(total_len == data_len)) {
                ThrowInfo(
                    ErrorCode::DataFormatBroken,
                    "index len is inconsistent after disassemble and assemble");
            }
            if (index_datas.count(prefix) > 0) {
                index_data_codec.codecs_.push_back(
                    std::move(index_datas[prefix]));
                compacted_files.insert(prefix);
            }
            index_data_codec.size_ = data_len;
        }
    }
    for (auto& index_data : index_datas) {
        if (compacted_files.find(index_data.first) == compacted_files.end()) {
            index_file_slices.insert({index_data.first, IndexDataCodec{}});
            auto& index_data_codec = index_file_slices.at(index_data.first);
            index_data_codec.size_ = index_data.second->PayloadSize();
            index_data_codec.codecs_.push_back(std::move(index_data.second));
        }
    }
    return index_file_slices;
}

void
AssembleIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas,
    BinarySet& index_binary_set) {
    auto index_file_slices = CompactIndexDatas(index_datas);
    AssembleIndexDatas(index_file_slices, index_binary_set);
}

void
AssembleIndexDatas(std::map<std::string, IndexDataCodec>& index_file_slices,
                   BinarySet& index_binary_set) {
    for (auto& [key, index_slices] : index_file_slices) {
        auto index_size = index_slices.size_;
        auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[index_size]);
        int64_t offset = 0;
        for (auto&& index_slice : index_slices.codecs_) {
            milvus::fastmem::FastMemcpy(buf.get() + offset,
                                        index_slice->PayloadData(),
                                        index_slice->PayloadSize());
            offset += index_slice->PayloadSize();
        }
        index_binary_set.Append(key, buf, index_size);
    }
}

}  // namespace milvus::index
