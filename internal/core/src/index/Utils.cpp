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

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <functional>
#include <iostream>
#include <unistd.h>
#include <google/protobuf/text_format.h>

#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/File.h"
#include "common/FieldData.h"
#include "common/Slice.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "storage/Util.h"
#include "knowhere/comp/index_param.h"

namespace milvus::index {

size_t
get_file_size(int fd) {
    struct stat s;
    fstat(fd, &s);
    return s.st_size;
}

std::vector<IndexType>
NM_List() {
    static std::vector<IndexType> ret{
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
    };
    return ret;
}

std::vector<IndexType>
BIN_List() {
    static std::vector<IndexType> ret{
        knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
        knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
    };
    return ret;
}

// TODO caiyd: should list supported list
std::vector<std::tuple<IndexType, MetricType>>
unsupported_index_combinations() {
    static std::vector<std::tuple<IndexType, MetricType>> ret{
        std::make_tuple(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                        knowhere::metric::L2),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::L2),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::COSINE),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::HAMMING),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::JACCARD),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::SUBSTRUCTURE),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                        knowhere::metric::SUPERSTRUCTURE),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::L2),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::COSINE),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::HAMMING),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::JACCARD),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::SUBSTRUCTURE),
        std::make_tuple(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                        knowhere::metric::SUPERSTRUCTURE),
    };
    return ret;
}

bool
is_in_bin_list(const IndexType& index_type) {
    return is_in_list<IndexType>(index_type, BIN_List);
}

bool
is_in_nm_list(const IndexType& index_type) {
    return is_in_list<IndexType>(index_type, NM_List);
}

bool
is_unsupported(const IndexType& index_type, const MetricType& metric_type) {
    return is_in_list<std::tuple<IndexType, MetricType>>(
        std::make_tuple(index_type, metric_type),
        unsupported_index_combinations);
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
        LOG_ERROR(err_message);
        throw std::logic_error(err_message);
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
        LOG_ERROR(err_message);
        throw std::logic_error(err_message);
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
        LOG_ERROR(err_message);
        throw std::logic_error(err_message);
    }
}

// TODO :: too ugly
storage::FieldDataMeta
GetFieldDataMetaFromConfig(const Config& config) {
    storage::FieldDataMeta field_data_meta;
    // set collection id
    auto collection_id =
        index::GetValueFromConfig<std::string>(config, index::COLLECTION_ID);
    AssertInfo(collection_id.has_value(),
               "collection id not exist in index config");
    field_data_meta.collection_id = std::stol(collection_id.value());

    // set partition id
    auto partition_id =
        index::GetValueFromConfig<std::string>(config, index::PARTITION_ID);
    AssertInfo(partition_id.has_value(),
               "partition id not exist in index config");
    field_data_meta.partition_id = std::stol(partition_id.value());

    // set segment id
    auto segment_id =
        index::GetValueFromConfig<std::string>(config, index::SEGMENT_ID);
    AssertInfo(segment_id.has_value(), "segment id not exist in index config");
    field_data_meta.segment_id = std::stol(segment_id.value());

    // set field id
    auto field_id =
        index::GetValueFromConfig<std::string>(config, index::FIELD_ID);
    AssertInfo(field_id.has_value(), "field id not exist in index config");
    field_data_meta.field_id = std::stol(field_id.value());

    return field_data_meta;
}

storage::IndexMeta
GetIndexMetaFromConfig(const Config& config) {
    storage::IndexMeta index_meta;
    // set segment id
    auto segment_id =
        index::GetValueFromConfig<std::string>(config, index::SEGMENT_ID);
    AssertInfo(segment_id.has_value(), "segment id not exist in index config");
    index_meta.segment_id = std::stol(segment_id.value());

    // set field id
    auto field_id =
        index::GetValueFromConfig<std::string>(config, index::FIELD_ID);
    AssertInfo(field_id.has_value(), "field id not exist in index config");
    index_meta.field_id = std::stol(field_id.value());

    // set index version
    auto index_version =
        index::GetValueFromConfig<std::string>(config, index::INDEX_VERSION);
    AssertInfo(index_version.has_value(),
               "index_version id not exist in index config");
    index_meta.index_version = std::stol(index_version.value());

    // set index id
    auto build_id =
        index::GetValueFromConfig<std::string>(config, index::INDEX_BUILD_ID);
    AssertInfo(build_id.has_value(), "build id not exist in index config");
    index_meta.build_id = std::stol(build_id.value());

    return index_meta;
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

void
AssembleIndexDatas(std::map<std::string, FieldDataPtr>& index_datas) {
    if (index_datas.find(INDEX_FILE_SLICE_META) != index_datas.end()) {
        auto slice_meta = index_datas.at(INDEX_FILE_SLICE_META);
        Config meta_data = Config::parse(
            std::string(static_cast<const char*>(slice_meta->Data()),
                        slice_meta->DataSize()));

        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
            // build index skip null value, so not need to set nullable == true
            auto new_field_data =
                storage::CreateFieldData(DataType::INT8, false, 1, total_len);

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                AssertInfo(index_datas.find(file_name) != index_datas.end(),
                           "lost index slice data");
                auto data = index_datas.at(file_name);
                auto len = data->DataSize();
                new_field_data->FillFieldData(data->Data(), len);
                index_datas.erase(file_name);
            }
            AssertInfo(
                new_field_data->IsFull(),
                "index len is inconsistent after disassemble and assemble");
            index_datas[prefix] = new_field_data;
        }
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
            auto new_field_data =
                storage::CreateFieldData(DataType::INT8, false, 1, total_len);

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                auto it = index_datas.find(file_name);
                AssertInfo(it != index_datas.end(), "lost index slice data");
                auto& channel = it->second;
                auto data_array = storage::CollectFieldDataChannel(channel);
                auto data = storage::MergeFieldData(data_array);
                auto len = data->DataSize();
                new_field_data->FillFieldData(data->Data(), len);
                index_datas.erase(file_name);
            }
            AssertInfo(
                new_field_data->IsFull(),
                "index len is inconsistent after disassemble and assemble");
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
            PanicInfo(ErrorCode::UnistdError,
                      "read data from fd error, returned read size is " +
                          std::to_string(size_read));
        }

        buf = static_cast<char*>(buf) + size_read;
        size -= static_cast<std::size_t>(size_read);
    }
}

bool
CheckAndUpdateKnowhereRangeSearchParam(const SearchInfo& search_info,
                                       const int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config) {
    const auto radius =
        index::GetValueFromConfig<float>(search_info.search_params_, RADIUS);
    if (!radius.has_value()) {
        return false;
    }

    search_config[RADIUS] = radius.value();
    // `range_search_k` is only used as one of the conditions for iterator early termination.
    // not gurantee to return exactly `range_search_k` results, which may be more or less.
    // set it to -1 will return all results in the range.
    search_config[knowhere::meta::RANGE_SEARCH_K] = topk;

    const auto range_filter =
        GetValueFromConfig<float>(search_info.search_params_, RANGE_FILTER);
    if (range_filter.has_value()) {
        search_config[RANGE_FILTER] = range_filter.value();
        CheckRangeSearchParam(
            search_config[RADIUS], search_config[RANGE_FILTER], metric_type);
    }

    const auto page_retain_order =
        GetValueFromConfig<bool>(search_info.search_params_, PAGE_RETAIN_ORDER);
    if (page_retain_order.has_value()) {
        search_config[knowhere::meta::RETAIN_ITERATOR_ORDER] =
            page_retain_order.value();
    }
    return true;
}

void inline SetBitset(void* bitset, uint32_t doc_id) {
    TargetBitmap* bitmap = static_cast<TargetBitmap*>(bitset);
    assert(doc_id < bitmap->size());
    (*bitmap)[doc_id] = true;
}

}  // namespace milvus::index
