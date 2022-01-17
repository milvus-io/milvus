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

#include "common/Schema.h"
#include "query/SubSearchResult.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"

namespace milvus::query {

SubSearchResult
BinarySearchBruteForce(const dataset::SearchDataset& dataset,
                       const void* chunk_data_raw,
                       int64_t size_per_chunk,
                       const faiss::BitsetView& bitset);

SubSearchResult
FloatSearchBruteForce(const dataset::SearchDataset& dataset,
                      const void* chunk_data_raw,
                      int64_t size_per_chunk,
                      const faiss::BitsetView& bitset);

}  // namespace milvus::query
