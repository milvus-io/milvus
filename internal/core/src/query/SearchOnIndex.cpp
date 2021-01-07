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

#include "SearchOnIndex.h"
namespace milvus::query {
SubQueryResult
SearchOnIndex(const dataset::FloatQueryDataset& query_dataset,
              const knowhere::VecIndex& indexing,
              const knowhere::Config& search_conf,
              const faiss::BitsetView& bitset) {
    auto num_queries = query_dataset.num_queries;
    auto topK = query_dataset.topk;
    auto dim = query_dataset.dim;
    auto metric_type = query_dataset.metric_type;

    auto dataset = knowhere::GenDataset(num_queries, dim, query_dataset.query_data);

    // NOTE: VecIndex Query API forget to add const qualifier
    // NOTE: use const_cast as a workaround
    auto& indexing_nonconst = const_cast<knowhere::VecIndex&>(indexing);
    auto ans = indexing_nonconst.Query(dataset, search_conf, bitset);

    auto dis = ans->Get<float*>(milvus::knowhere::meta::DISTANCE);
    auto uids = ans->Get<int64_t*>(milvus::knowhere::meta::IDS);

    SubQueryResult sub_qr(num_queries, topK, metric_type);
    std::copy_n(dis, num_queries * topK, sub_qr.get_values());
    std::copy_n(uids, num_queries * topK, sub_qr.get_labels());
    return sub_qr;
}

}  // namespace milvus::query
