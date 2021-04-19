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
#include <faiss/utils/BinaryDistance.h>
#include "segcore/ConcurrentVector.h"
#include "common/Schema.h"
#include "query/SubQueryResult.h"

namespace milvus::query {
using MetricType = faiss::MetricType;

namespace dataset {
struct BinaryQueryDataset {
    MetricType metric_type;
    int64_t num_queries;
    int64_t topk;
    int64_t code_size;
    const uint8_t* query_data;
};

}  // namespace dataset

SubQueryResult
BinarySearchBruteForce(const dataset::BinaryQueryDataset& query_dataset,
                       const uint8_t* binary_chunk,
                       int64_t chunk_size,
                       const faiss::BitsetView& bitset = nullptr);

}  // namespace milvus::query
