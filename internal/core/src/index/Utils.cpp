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

#include <google/protobuf/text_format.h>
#include "common/FastMem.h"
#include <sys/stat.h>
#include <unistd.h>
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
#include "fmt/core.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "storage/IndexData.h"
#include "storage/Util.h"

namespace milvus::index {

size_t
get_file_size(int fd) {
    struct stat s;
    fstat(fd, &s);
    return s.st_size;
}

bool
CheckKeyInConfig(const Config& cfg, const std::string& key) {
    return cfg.contains(key);
}

void
ParseFromString(google::protobuf::Message& params, const std::string& str) {
    auto ok = google::protobuf::TextFormat::ParseFromString(str, &params);
    AssertInfo(ok, "failed to parse params from string");
}

int64_t
GetDimFromConfig(const Config& config) {
    auto dimension = GetValueFromConfig<std::string>(config, "dim");
    AssertInfo(dimension.has_value(), "dimension not exist in config");
    try {
        return (std::stoi(dimension.value()));
    } catch (const std::logic_error& e) {
        auto err_message = fmt::format(
            "invalided dimension:{}, error:{}", dimension.value(), e.what());
        LOG_ERROR("{}", err_message);
        ThrowInfo(ErrorCode::UnexpectedError, "{}", std::string(err_message));
    }
}

std::string
GetMetricTypeFromConfig(const Config& config) {
    auto metric_type = GetValueFromConfig<std::string>(config, "metric_type");
    AssertInfo(metric_type.has_value(), "metric_type not exist in config");
    return metric_type.value();
}

std::string
GetIndexTypeFromConfig(const Config& config) {
    auto index_type = GetValueFromConfig<std::string>(config, "index_type");
    AssertInfo(index_type.has_value(), "index_type not exist in config");
    return index_type.value();
}

IndexVersion
GetIndexEngineVersionFromConfig(const Config& config) {
    auto index_engine_version =
        GetValueFromConfig<std::string>(config, INDEX_ENGINE_VERSION);
    AssertInfo(index_engine_version.has_value(),
               "index_engine not exist in config");
    try {
        return (std::stoi(index_engine_version.value()));
    } catch (const std::logic_error& e) {
        auto err_message =
            fmt::format("invalided index engine version:{}, error:{}",
                        index_engine_version.value(),
                        e.what());
        LOG_ERROR("{}", err_message);
        ThrowInfo(ErrorCode::UnexpectedError, "{}", std::string(err_message));
    }
}

int32_t
GetBitmapCardinalityLimitFromConfig(const Config& config) {
    auto bitmap_limit = GetValueFromConfig<std::string>(
        config, index::BITMAP_INDEX_CARDINALITY_LIMIT);
    AssertInfo(bitmap_limit.has_value(),
               "bitmap cardinality limit not exist in config");
    try {
        return (std::stoi(bitmap_limit.value()));
    } catch (const std::logic_error& e) {
        auto err_message = fmt::format("invalided bitmap limit:{}, error:{}",
                                       bitmap_limit.value(),
                                       e.what());
        LOG_ERROR("{}", err_message);
        ThrowInfo(ErrorCode::UnexpectedError, "{}", std::string(err_message));
    }
}

std::string
GetLowCardinalityFamilyFromConfig(const Config& config) {
    auto type = GetValueFromConfig<std::string>(
        config, HYBRID_LOW_CARDINALITY_INDEX_TYPE);
    if (!type.has_value()) {
        return families::kBitmap;
    }
    if (*type == "BITMAP") {
        return families::kBitmap;
    }
    if (*type == "STLSORT" || *type == ASCENDING_SORT) {
        return families::kSort;
    }
    if (*type == "MARISA" || *type == MARISA_TRIE ||
        *type == MARISA_TRIE_UPPER) {
        return families::kMarisa;
    }
    if (*type == "INVERTED" || *type == INVERTED_INDEX_TYPE) {
        return families::kInverted;
    }
    AssertInfo(false, "unsupported hybrid scalar index type: {}", *type);
    return {};
}

std::string
GetHighCardinalityFamilyFromConfig(const Config& config) {
    auto type = GetValueFromConfig<std::string>(
        config, HYBRID_HIGH_CARDINALITY_INDEX_TYPE);
    if (!type.has_value()) {
        return families::kSort;
    }
    Config copy = config;
    copy[HYBRID_LOW_CARDINALITY_INDEX_TYPE] = *type;
    return GetLowCardinalityFamilyFromConfig(copy);
}

Config
ParseConfigFromIndexParams(
    const std::map<std::string, std::string>& index_params) {
    Config config;
    for (auto& p : index_params) {
        config[p.first] = p.second;
    }

    return config;
}

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

IndexDataCodec
CompactIndexDatasByKey(
    const std::string& key,
    std::unique_ptr<storage::DataCodec> slice_meta,
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas) {
    Config meta_data = Config::parse(
        std::string(reinterpret_cast<const char*>(slice_meta->PayloadData()),
                    slice_meta->PayloadSize()));

    int slice_num = 0;
    size_t total_len = 0;
    bool found = false;
    for (const auto& item : meta_data[META]) {
        if (item[NAME] == key) {
            slice_num = item[SLICE_NUM];
            total_len = static_cast<size_t>(item[TOTAL_LEN]);
            found = true;
            break;
        }
    }

    if (!found) {
        return {};
    }

    IndexDataCodec index_data_codec;
    size_t data_len = 0;
    for (auto i = 0; i < slice_num; ++i) {
        std::string file_name = GenSlicedFileName(key, i);
        auto it = index_datas.find(file_name);
        if (!(it != index_datas.end())) {
            ThrowInfo(ErrorCode::DataFormatBroken, "lost index slice data");
        }
        index_data_codec.codecs_.push_back(std::move(it->second));
        data_len += index_data_codec.codecs_.back()->PayloadSize();
    }
    if (!(total_len == data_len)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "index len is inconsistent after disassemble and assemble");
    }
    index_data_codec.size_ = data_len;
    return index_data_codec;
}

std::unique_ptr<storage::DataCodec>
AssembleIndexDataCodec(const IndexDataCodec& index_slices) {
    AssertInfo(index_slices.size_ >= 0, "index data size is invalid");
    auto index_size = index_slices.size_;
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[index_size]);
    int64_t offset = 0;
    for (const auto& index_slice : index_slices.codecs_) {
        milvus::fastmem::FastMemcpy(buf.get() + offset,
                                    index_slice->PayloadData(),
                                    index_slice->PayloadSize());
        offset += index_slice->PayloadSize();
    }
    AssertInfo(offset == index_size,
               "index len is inconsistent after disassemble and assemble");

    auto index_data =
        std::make_unique<storage::IndexData>(buf.get(), index_size);
    index_data->SetData(std::move(buf));
    return index_data;
}

std::unique_ptr<storage::DataCodec>
AssembleIndexDataCodec(IndexDataCodec&& index_slices) {
    AssertInfo(index_slices.size_ >= 0, "index data size is invalid");
    if (index_slices.codecs_.size() == 1) {
        auto index_data = std::move(index_slices.codecs_.front());
        if (!(index_data->PayloadSize() == index_slices.size_)) {
            ThrowInfo(
                ErrorCode::DataFormatBroken,
                "index len is inconsistent after disassemble and assemble");
        }
        return index_data;
    }
    return AssembleIndexDataCodec(index_slices);
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

void
AssembleIndexDatas(std::map<std::string, FieldDataChannelPtr>& index_datas,
                   std::unordered_map<std::string, FieldDataPtr>& result) {
    if (auto meta_iter = index_datas.find(INDEX_FILE_SLICE_META);
        meta_iter != index_datas.end()) {
        auto raw_metadata_array =
            storage::CollectFieldDataChannel(meta_iter->second);
        auto raw_metadata = storage::MergeFieldData(raw_metadata_array);
        result[INDEX_FILE_SLICE_META] = raw_metadata;
        index_datas.erase(INDEX_FILE_SLICE_META);
        Config metadata = Config::parse(
            std::string(static_cast<const char*>(raw_metadata->Data()),
                        raw_metadata->DataSize()));

        for (auto& item : metadata[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
            // build index skip null value, so not need to set nullable == true
            auto new_field_data = storage::CreateFieldData(
                DataType::INT8, DataType::NONE, false, 1, total_len);

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                auto it = index_datas.find(file_name);
                if (!(it != index_datas.end())) {
                    ThrowInfo(ErrorCode::DataFormatBroken,
                              "lost index slice data");
                }
                auto& channel = it->second;
                auto data_array = storage::CollectFieldDataChannel(channel);
                auto data = storage::MergeFieldData(data_array);
                auto len = data->DataSize();
                new_field_data->FillFieldData(data->Data(), len);
                index_datas.erase(file_name);
            }
            if (!(new_field_data->IsFull())) {
                ThrowInfo(
                    ErrorCode::DataFormatBroken,
                    "index len is inconsistent after disassemble and assemble");
            }
            result[prefix] = new_field_data;
        }
    }
    for (auto& [key, channel] : index_datas) {
        if (key == INDEX_FILE_SLICE_META) {
            continue;
        }

        auto data_array = storage::CollectFieldDataChannel(channel);
        auto data = storage::MergeFieldData(data_array);
        result[key] = data;
    }
}

void
ReadDataFromFD(int fd, void* buf, size_t size, size_t chunk_size) {
    lseek(fd, 0, SEEK_SET);
    while (size != 0) {
        const size_t count = (size < chunk_size) ? size : chunk_size;
        const ssize_t size_read = read(fd, buf, count);
        if (size_read != count) {
            ThrowInfo(ErrorCode::UnistdError,
                      "read data from fd error, returned read size is {}",
                      size_read);
        }

        buf = static_cast<char*>(buf) + size_read;
        size -= static_cast<std::size_t>(size_read);
    }
}

}  // namespace milvus::index
