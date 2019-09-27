// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>

#include "IndexIVFPQ.h"
#include "knowhere/common/Exception.h"
#include "knowhere/adapter/VectorAdapter.h"

namespace zilliz {
namespace knowhere {

IndexModelPtr IVFPQ::Train(const DatasetPtr &dataset, const Config &config) {
    auto build_cfg = std::dynamic_pointer_cast<IVFPQCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid(); // throw exception
    }

    GETTENSOR(dataset)

    faiss::Index *coarse_quantizer = new faiss::IndexFlat(dim, GetMetricType(build_cfg->metric_type));
    auto index = std::make_shared<faiss::IndexIVFPQ>(coarse_quantizer, dim,
                                                     build_cfg->nlist, build_cfg->m, build_cfg->nbits);
    index->train(rows, (float *) p_data);

    return std::make_shared<IVFIndexModel>(index);
}

std::shared_ptr<faiss::IVFSearchParameters> IVFPQ::GenParams(const Config &config) {
    auto params = std::make_shared<faiss::IVFPQSearchParameters>();
    auto search_cfg = std::dynamic_pointer_cast<IVFPQCfg>(config);
    params->nprobe = search_cfg->nprobe;
//    params->scan_table_threshold = conf->scan_table_threhold;
//    params->polysemous_ht = conf->polysemous_ht;
//    params->max_codes = conf->max_codes;

    return params;
}

VectorIndexPtr IVFPQ::Clone_impl(const std::shared_ptr<faiss::Index> &index) {
    return std::make_shared<IVFPQ>(index);
}

} // knowhere
} // zilliz
