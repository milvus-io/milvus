// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

#include "common/Types.h"
#include "index/contracts/query/IVectorReader.h"
#include "knowhere/config.h"

// Validate and prepare knowhere range-search parameters. Kept in the vector
// family so shared scalar helpers do not include vector search configuration.
// topk and metric are explicit because iterator callers may use a batch size
// instead of the request's topk.

namespace milvus::index {

// Returns false when the query has no RADIUS (i.e. it is not a range search) and
// leaves `search_config` untouched; otherwise fills RADIUS / RANGE_SEARCH_K and,
// when present, RANGE_FILTER (validated by `CheckRangeSearchParam`,
// `common/RangeSearchHelper.h` — L0, stays where it is) and
// RETAIN_ITERATOR_ORDER.
bool
CheckAndUpdateKnowhereRangeSearchParam(const VectorSearchParams& params,
                                       int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config);

}  // namespace milvus::index
