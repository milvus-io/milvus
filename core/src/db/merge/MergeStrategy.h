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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "db/Types.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshot.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

using Partition2SegmentsMap = std::map<snapshot::ID_TYPE, snapshot::IDS_TYPE>;
using SegmentGroups = std::vector<snapshot::IDS_TYPE>;

class MergeStrategy {
 public:
    virtual Status
    RegroupSegments(const snapshot::ScopedSnapshotT& ss, const Partition2SegmentsMap& part2segment,
                    SegmentGroups& groups) = 0;
};  // MergeStrategy

using MergeStrategyPtr = std::shared_ptr<MergeStrategy>;

}  // namespace engine
}  // namespace milvus
