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

#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/common/Exception.h"

#include <faiss/Index.h>

namespace milvus {
namespace knowhere {

faiss::MetricType
GetMetricType(const std::string& type) {
    if (type == Metric::L2) {
        return faiss::METRIC_L2;
    }
    if (type == Metric::IP) {
        return faiss::METRIC_INNER_PRODUCT;
    }
    if (type == Metric::JACCARD) {
        return faiss::METRIC_Jaccard;
    }
    if (type == Metric::TANIMOTO) {
        return faiss::METRIC_Tanimoto;
    }
    if (type == Metric::HAMMING) {
        return faiss::METRIC_Hamming;
    }
    if (type == Metric::SUBSTRUCTURE) {
        return faiss::METRIC_Substructure;
    }
    if (type == Metric::SUPERSTRUCTURE) {
        return faiss::METRIC_Superstructure;
    }

    KNOWHERE_THROW_MSG("Metric type is invalid");
}

}  // namespace knowhere
}  // namespace milvus
