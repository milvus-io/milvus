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

#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

// 1. SIMPLE
//    merge in old way, merge segment one by one, stop merge until segment row count exceed segment_row_count
// 2. LAYERED
//    distribute segments to several groups according to segment_row_count
//    assume segment_row_count = 100000
//    firstly, define layers by row count: 100000, 20000, 4000
//    if segment row count between 0~4000, put it into layer "4000"
//    if segment row count between 4000~20000, put it into layer "20000"
//    if segment row count between 20000~100000, put it into layer "100000"
//    segment row count greater than 100000 will be ignored
//    secondly, merge segments for each group
//    third, if some segment's create time is 30 seconds ago, and it still un-merged, force merge with upper layer
// 3. ADAPTIVE
//    merge segments to fit the segment_row_count
//    assume segment_row_count = 100000
//    segment_1 row count = 80000
//    segment_2 row count = 60000
//    segment_3 row count = 30000
//    segment_1 and segment_3 will be merged, since there row sum = 110000,
//    that is much closer than row sum of segment_1 and segment_2, 140000
enum class MergeStrategyType {
    SIMPLE = 1,
    LAYERED = 2,
    ADAPTIVE = 3,
};

class MergeManager {
 public:
    virtual Status
    MergeSegments(int64_t collection_id, MergeStrategyType type = MergeStrategyType::LAYERED) = 0;
};  // MergeManager

using MergeManagerPtr = std::shared_ptr<MergeManager>;

}  // namespace engine
}  // namespace milvus
