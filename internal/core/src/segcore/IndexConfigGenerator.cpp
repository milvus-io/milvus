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
#include "knowhere/comp/index_param.h"
#include "log/Log.h"

namespace milvus::segcore {
VecIndexConfig::VecIndexConfig(const int64_t max_index_row_cout,
                               const FieldIndexMeta& index_meta_,
                               const SegcoreConfig& config,
                               const SegmentType& segment_type,
                               const bool is_sparse)
    : max_index_row_count_(max_index_row_cout),
      config_(config),
      is_sparse_(is_sparse) {
    origin_index_type_ = index_meta_.GetIndexType();
    metric_type_ = index_meta_.GeMetricType();
    // For Dense vector, use IVFFLAT_CC as the growing and temp index type.
    //
    // For Sparse vector, use SPARSE_WAND_CC for INDEX_SPARSE_WAND index, or use
    // SPARSE_INVERTED_INDEX_CC for INDEX_SPARSE_INVERTED_INDEX/other sparse
    // index types as the growing and temp index type.

    if (origin_index_type_ == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        index_type_ = knowhere::IndexEnum::INDEX_SPARSE_WAND_CC;
    } else if (is_sparse_) {
        index_type_ = knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX_CC;
    } else {
        index_type_ = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    }
    build_params_[knowhere::meta::METRIC_TYPE] = metric_type_;
    build_params_[knowhere::indexparam::NLIST] =
        std::to_string(config_.get_nlist());
    build_params_[knowhere::indexparam::SSIZE] = std::to_string(
        std::max((int)(config_.get_chunk_rows() / config_.get_nlist()), 48));

    if (is_sparse) {
        const auto& index_params = index_meta_.GetIndexParams();

        if (auto algo_it =
                index_params.find(knowhere::indexparam::INVERTED_INDEX_ALGO);
            algo_it != index_params.end()) {
            build_params_[knowhere::indexparam::INVERTED_INDEX_ALGO] =
                algo_it->second;
        }

        if (metric_type_ == knowhere::metric::BM25) {
            build_params_[knowhere::meta::BM25_K1] =
                index_params.at(knowhere::meta::BM25_K1);
            build_params_[knowhere::meta::BM25_B] =
                index_params.at(knowhere::meta::BM25_B);
            build_params_[knowhere::meta::BM25_AVGDL] =
                index_params.at(knowhere::meta::BM25_AVGDL);
        }
    }

    search_params_[knowhere::indexparam::NPROBE] =
        std::to_string(config_.get_nprobe());

    // note for sparse vector index: drop_ratio_build is not allowed for growing
    // segment index.
    LOG_INFO(
        "VecIndexConfig: origin_index_type={}, index_type={}, metric_type={}, "
        "config={}",
        origin_index_type_,
        index_type_,
        metric_type_,
        build_params_.dump());
}

int64_t
VecIndexConfig::GetBuildThreshold() const noexcept {
    // For sparse, do not impose a threshold and start using index with any
    // number of rows. Unlike dense vector index, growing sparse vector index
    // does not require a minimum number of rows to train.
    if (is_sparse_) {
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
VecIndexConfig::GetSearchConf(const SearchInfo& searchInfo) {
    SearchInfo searchParam(searchInfo);
    searchParam.metric_type_ = metric_type_;
    searchParam.search_params_ = search_params_;
    for (auto& key : maintain_params) {
        if (searchInfo.search_params_.contains(key)) {
            searchParam.search_params_[key] = searchInfo.search_params_[key];
        }
    }

    if (metric_type_ == knowhere::metric::BM25) {
        searchParam.search_params_[knowhere::meta::BM25_AVGDL] =
            searchInfo.search_params_[knowhere::meta::BM25_AVGDL];
    }
    return searchParam;
}

}  // namespace milvus::segcore