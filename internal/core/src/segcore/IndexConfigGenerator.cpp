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

#include "IndexConfigGenerator.h"
#include "log/Log.h"

namespace milvus::segcore {
namespace {
static constexpr float kHnswDefaultCompressRatio = 0.5;
static constexpr int64_t kMinNprobe = 1;
static constexpr int64_t kDefaultNprobe = 16;
static constexpr const char* kSearchLevelKey = "level";
}  // namespace

SearchParamsGenerator::SearchParamsGenerator(const int64_t nlist,
                                             const int64_t search_granularity,
                                             const int64_t n_rows)
    : nlist_(nlist), search_granularity_(search_granularity), n_rows_(n_rows) {
    min_nprobe_ = std::min(kMinNprobe, nlist_);
    slots_num_ = slots_factor.size() + search_granularity_;
    slot_offest_ = float(nlist - min_nprobe_) / float(slots_num_);
}

int64_t
SearchParamsGenerator::GetNprobe(uint64_t topk, uint64_t search_level) {
    uint64_t slot_id = 0;
    float key = float(topk) / float(n_rows_);
    for (; slot_id < slots_factor.size(); slot_id++) {
        if (key < slot_id) {
            break;
        }
    }
    slot_id += (search_level - 1) % search_granularity_;
    return int(slot_id * slot_offest_) + min_nprobe_;
}

knowhere::Json
SearchParamsGenerator::GetSearchConfig(const SearchInfo& search_info) {
    knowhere::Json search_params;
    if (search_info.group_by_field_id_.has_value()) {
        // use default nprobe for iterator
        search_params[knowhere::indexparam::NPROBE] =
            std::to_string(kDefaultNprobe);
    } else if (!search_info.search_params_.contains(RADIUS)) {
        uint64_t topk = search_info.topk_;
        if (search_info.search_params_.contains(kSearchLevelKey)) {
            auto level_str =
                search_info.search_params_[kSearchLevelKey].get<std::string>();
            auto level = std::stoi(level_str.c_str(), nullptr);
            search_params[knowhere::indexparam::NPROBE] =
                std::to_string(GetNprobe(topk, level));
        } else {
            search_params[knowhere::indexparam::NPROBE] =
                std::to_string(GetNprobe(topk));
        }
    }
    return search_params;
}

VecIndexConfig::VecIndexConfig(const int64_t max_index_row_count,
                               const FieldIndexMeta& index_meta_,
                               const SegcoreConfig& config,
                               const SegmentType& segment_type)
    : max_index_row_count_(max_index_row_count),
      config_(config),
      search_params_generater_(config.get_nlist(),
                               config.get_search_granularity(),
                               max_index_row_count) {
    // todo: intermin index config only support vector_float for now, add more data type in future
    origin_index_type_ = index_meta_.GetIndexType();
    metric_type_ = index_meta_.GeMetricType();
    // Currently for dense vector index, if the segment is growing, we use IVFCC
    // as the index type; if the segment is sealed but its index has not been
    // built by the index node, we use IVFFLAT as the temp index type and
    // release it once the index node has finished building the index and query
    // node has loaded it.

    // But for sparse vector index(INDEX_SPARSE_INVERTED_INDEX and
    // INDEX_SPARSE_WAND), those index themselves can be used as the temp index
    // type, so we can avoid the extra step of "releast temp and load".
    auto vec_compress_ratio = config.get_vec_compress_ratio();
    LOG_INFO("VecIndexConfig: use vector compress ratio :{}",
             vec_compress_ratio);
    if (origin_index_type_ ==
            knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
        origin_index_type_ == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        index_type_ = origin_index_type_;
    } else {
        SetDenseVecIndexType(vec_compress_ratio);
    }
    build_params_[knowhere::meta::METRIC_TYPE] = metric_type_;
    build_params_[knowhere::indexparam::NLIST] =
        std::to_string(config_.get_nlist());
    build_params_[knowhere::indexparam::SSIZE] = std::to_string(
        std::max((int)(config_.get_chunk_rows() / config_.get_nlist()), 48));
    // note for sparse vector index: drop_ratio_build is not allowed for growing
    // segment index.
    auto code_size_res = GetVecCodeSize(vec_compress_ratio);
    if (code_size_res.has_value()) {
        build_params_[knowhere::indexparam::CODE_SIZE] =
            std::to_string(code_size_res.value());
    }
    LOG_INFO(
        "VecIndexConfig: origin_index_type={}, index_type={}, metric_type={}",
        origin_index_type_,
        index_type_,
        metric_type_);
}

int64_t
VecIndexConfig::GetBuildThreshold() const noexcept {
    // For sparse, do not impose a threshold and start using index with any
    // number of rows. Unlike dense vector index, growing sparse vector index
    // does not require a minimum number of rows to train.
    if (origin_index_type_ ==
            knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
        origin_index_type_ == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        return 0;
    }
    assert(VecIndexConfig::index_build_ratio.count(index_type_));
    auto ratio = VecIndexConfig::index_build_ratio.at(index_type_);
    assert(ratio >= 0.0 && ratio < 1.0);
    return std::max(int64_t(max_index_row_count_ * ratio),
                    config_.get_nlist() * 39);
}

knowhere::IndexType
VecIndexConfig::GetIndexType() noexcept {
    return index_type_;
}

knowhere::MetricType
VecIndexConfig::GetMetricType() noexcept {
    return metric_type_;
}

knowhere::Json
VecIndexConfig::GetBuildBaseParams() {
    return build_params_;
}

SearchInfo
VecIndexConfig::GetSearchConf(const SearchInfo& search_info) {
    SearchInfo searchParam(search_info);
    searchParam.metric_type_ = metric_type_;
    searchParam.search_params_ =
        search_params_generater_.GetSearchConfig(search_info);
    for (auto& key : maintain_params) {
        if (search_info.search_params_.contains(key)) {
            searchParam.search_params_[key] = search_info.search_params_[key];
        }
    }
    return searchParam;
}

std::optional<uint32_t>
VecIndexConfig::GetVecCodeSize(float vec_compress_ratio) {
    if (vec_compress_ratio >= 1.0)
        return std::nullopt;
    auto vec_component_size =
        uint32_t(vec_compress_ratio * 32);  // assume data type is float32
    if (vec_component_size <= 4) {
        return 4;
    } else if (vec_component_size <= 6) {
        return 6;
    } else if (vec_component_size <= 8) {
        return 8;
    } else {
        return 16;
    }
}

void
VecIndexConfig::SetDenseVecIndexType(float vec_compress_ratio) {
    if (vec_compress_ratio >= 1.0) {
        index_type_ = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    } else {
        index_type_ = knowhere::IndexEnum::INDEX_FAISS_IVFSQ_CC;
    }
}

uint64_t
VecIndexConfig::EstimateBuildBinlogIndexMemoryInBytes(uint32_t raw_data_size,
                                                      float build_expand_rate) {
    if (origin_index_type_ == knowhere::IndexEnum::INDEX_FAISS_IDMAP ||
        max_index_row_count_ < GetBuildThreshold()) {
        // not build binlog index
        return raw_data_size;
    }
    auto build_mem = raw_data_size * build_expand_rate;
    // todo: estimate sparse vector
    if (GetIndexType() == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
        build_mem += raw_data_size;
    } else {
        auto vec_compress_ratio = config_.get_vec_compress_ratio();
        auto code_size =
            GetVecCodeSize(vec_compress_ratio).value_or(8 * sizeof(float));
        build_mem += raw_data_size * ((float)code_size / (8 * sizeof(float)));
    }

    return build_mem;
}
}  // namespace milvus::segcore