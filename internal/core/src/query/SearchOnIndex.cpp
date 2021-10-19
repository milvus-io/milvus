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
SubSearchResult
SearchOnIndex(const dataset::SearchDataset& search_dataset,
              const knowhere::VecIndex& indexing,
              const knowhere::Config& search_conf,
              const faiss::BitsetView& bitset) {
    auto num_queries = search_dataset.num_queries;
    auto topK = search_dataset.topk;
    auto dim = search_dataset.dim;
    auto metric_type = search_dataset.metric_type;
    auto round_decimal = search_dataset.round_decimal;
    auto dataset = knowhere::GenDataset(num_queries, dim, search_dataset.query_data);

    // NOTE: VecIndex Query API forget to add const qualifier
    // NOTE: use const_cast as a workaround
    auto& indexing_nonconst = const_cast<knowhere::VecIndex&>(indexing);
    auto ans = indexing_nonconst.Query(dataset, search_conf, bitset);

    auto dis = ans->Get<float*>(milvus::knowhere::meta::DISTANCE);
    auto uids = ans->Get<int64_t*>(milvus::knowhere::meta::IDS);

    SubSearchResult sub_qr(num_queries, topK, metric_type, round_decimal);
    std::copy_n(dis, num_queries * topK, sub_qr.get_values());
    std::copy_n(uids, num_queries * topK, sub_qr.get_labels());
    sub_qr.round_values();
    return sub_qr;
}

}  // namespace milvus::query
