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

#include <stdint.h>
#include <cstddef>
#include <functional>

#include "common/BitsetView.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "segcore/SegmentGrowingImpl.h"

namespace milvus::query {

// Test-only synchronization hook invoked immediately after the growing vector
// column's chunk snapshot is acquired. Production never sets it. Tests use it
// to place a concurrent insert precisely between snapshot acquisition and the
// rest of the search path.
using SearchOnGrowingAfterChunkSnapshotHook = std::function<void()>;

void
SetSearchOnGrowingAfterChunkSnapshotHookForTest(
    SearchOnGrowingAfterChunkSnapshotHook hook);

void
SearchOnGrowing(const segcore::SegmentGrowingImpl& segment,
                const SearchInfo& info,
                const void* query_data,
                const size_t* query_offsets,
                int64_t num_queries,
                Timestamp timestamp,
                const BitsetView& bitset,
                milvus::OpContext* op_context,
                SearchResult& search_result);

}  // namespace milvus::query
