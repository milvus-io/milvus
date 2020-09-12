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
#include <utility>
#include <vector>

#include "db/Types.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

struct SegmentInfo {
    SegmentInfo(snapshot::ID_TYPE id, int64_t row_count, snapshot::TS_TYPE create_on)
        : id_(id), row_count_(row_count), create_on_(create_on) {
    }

    snapshot::ID_TYPE id_ = 0;
    int64_t row_count_ = 0;
    snapshot::TS_TYPE create_on_ = 0;
};

using SegmentInfoList = std::vector<SegmentInfo>;
using Partition2SegmentsMap = std::unordered_map<snapshot::ID_TYPE, SegmentInfoList>;
using SegmentGroups = std::vector<snapshot::IDS_TYPE>;

class MergeStrategy {
 public:
    virtual Status
    RegroupSegments(const Partition2SegmentsMap& part2segment, int64_t row_per_segment, SegmentGroups& groups) = 0;
};  // MergeStrategy

using MergeStrategyPtr = std::shared_ptr<MergeStrategy>;

}  // namespace engine
}  // namespace milvus
