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
#include <optional>
#include "segcore/SegmentSmallIndex.h"
#include <deque>
#include <boost/dynamic_bitset.hpp>

namespace milvus::query {
using BitmapChunk = boost::dynamic_bitset<>;
using BitmapSimple = std::deque<BitmapChunk>;

// note: c++17 don't support optional ref
Status
QueryBruteForceImpl(const segcore::SegmentSmallIndex& segment,
                    const QueryInfo& info,
                    const float* query_data,
                    int64_t num_queries,
                    Timestamp timestamp,
                    std::optional<const BitmapSimple*> bitmap_opt,
                    QueryResult& results);

Status
BinaryQueryBruteForceImpl(const segcore::SegmentSmallIndex& segment,
                          const query::QueryInfo& info,
                          const uint8_t* query_data,
                          int64_t num_queries,
                          Timestamp timestamp,
                          std::optional<const BitmapSimple*> bitmaps_opt,
                          QueryResult& results);
}  // namespace milvus::query
