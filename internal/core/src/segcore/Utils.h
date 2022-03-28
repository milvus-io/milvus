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

#include <stdlib.h>
#include <string>
#include <exception>
#include <stdexcept>
#include <knowhere/common/MetricType.h>

namespace milvus::segcore {

static inline constexpr const char*
MetricTypeToString(faiss::MetricType metric_type) {
    switch (metric_type) {
        case faiss::MetricType::METRIC_INNER_PRODUCT:
            return "METRIC_INNER_PRODUCT";
        case faiss::MetricType::METRIC_L2:
            return "METRIC_L2";
        case faiss::MetricType::METRIC_L1:
            return "METRIC_L1";
        case faiss::MetricType::METRIC_Linf:
            return "METRIC_Linf";
        case faiss::MetricType::METRIC_Lp:
            return "METRIC_Lp";
        case faiss::MetricType::METRIC_Jaccard:
            return "METRIC_Jaccard";
        case faiss::MetricType::METRIC_Tanimoto:
            return "METRIC_Tanimoto";
        case faiss::MetricType::METRIC_Hamming:
            return "METRIC_Hamming";
        case faiss::MetricType::METRIC_Substructure:
            return "METRIC_Substructure";
        case faiss::MetricType::METRIC_Superstructure:
            return "METRIC_Superstructure";
        case faiss::MetricType::METRIC_Canberra:
            return "METRIC_Canberra";
        case faiss::MetricType::METRIC_BrayCurtis:
            return "METRIC_BrayCurtis";
        case faiss::MetricType::METRIC_JensenShannon:
            return "METRIC_JensenShannon";
        default:
            return "Unsupported";
    }
}

}  // namespace milvus::segcore
