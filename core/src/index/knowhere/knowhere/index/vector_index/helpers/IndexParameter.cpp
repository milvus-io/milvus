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

namespace knowhere {

faiss::MetricType
GetMetricType(METRICTYPE& type) {
    if (type == METRICTYPE::L2) {
        return faiss::METRIC_L2;
    }
    if (type == METRICTYPE::IP) {
        return faiss::METRIC_INNER_PRODUCT;
    }
    // binary only
    if (type == METRICTYPE::JACCARD) {
        return faiss::METRIC_Jaccard;
    }
    if (type == METRICTYPE::TANIMOTO) {
        return faiss::METRIC_Tanimoto;
    }
    if (type == METRICTYPE::HAMMING) {
        return faiss::METRIC_Hamming;
    }

    KNOWHERE_THROW_MSG("Metric type is invalid");
}

std::stringstream
IVFCfg::DumpImpl() {
    auto ss = Cfg::DumpImpl();
    ss << ", nlist: " << nlist << ", nprobe: " << nprobe;
    return ss;
}

std::stringstream
IVFSQCfg::DumpImpl() {
    auto ss = IVFCfg::DumpImpl();
    ss << ", nbits: " << nbits;
    return ss;
}

std::stringstream
NSGCfg::DumpImpl() {
    auto ss = IVFCfg::DumpImpl();
    ss << ", knng: " << knng << ", search_length: " << search_length << ", out_degree: " << out_degree
       << ", candidate: " << candidate_pool_size;
    return ss;
}

}  // namespace knowhere
