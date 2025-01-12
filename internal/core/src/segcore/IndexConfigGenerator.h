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

#include "common/Types.h"
#include "common/IndexMeta.h"
#include "knowhere/config.h"
#include "SegcoreConfig.h"
#include "common/QueryInfo.h"
#include "common/type_c.h"

namespace milvus::segcore {

enum class IndexConfigLevel {
    UNKNOWN = 0,
    SUPPORT = 1,
    COMPATIBLE = 2,
    SYSTEM_ASSIGN = 3
};

// this is the config used for generating growing index or the temp sealed index
// when the segment is sealed before the index is built.
class VecIndexConfig {
    inline static const std::map<std::string, double> index_build_ratio = {
        {knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC, 0.1},
        {knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR, 0.1}};

    inline static const std::unordered_set<std::string> maintain_params = {
        "radius", "range_filter", "drop_ratio_search", "dim_max_score_ratio"};

 public:
    VecIndexConfig(const int64_t max_index_row_count,
                   const FieldIndexMeta& index_meta_,
                   const SegcoreConfig& config,
                   const SegmentType& segment_type,
                   const bool is_sparse);

    int64_t
    GetBuildThreshold() const noexcept;

    knowhere::IndexType
    GetIndexType() noexcept;

    knowhere::MetricType
    GetMetricType() noexcept;

    knowhere::Json
    GetBuildBaseParams();

    SearchInfo
    GetSearchConf(const SearchInfo& searchInfo);

 private:
    const SegcoreConfig& config_;

    int64_t max_index_row_count_;

    knowhere::IndexType origin_index_type_;

    knowhere::IndexType index_type_;

    knowhere::MetricType metric_type_;

    bool is_sparse_;

    knowhere::Json build_params_;

    knowhere::Json search_params_;
};
}  // namespace milvus::segcore