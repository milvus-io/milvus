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

#include "db/snapshot/SnapshotPolicy.h"
#include "db/Utils.h"

namespace milvus {
namespace engine {
namespace snapshot {

SnapshotNumPolicy::SnapshotNumPolicy(size_t num) : num_(num) {
}

bool
SnapshotNumPolicy::ShouldEject(const MapT& ids) {
    bool should = true;
    if (ids.size() <= num_) {
        should = false;
    }
    return should;
}

SnapshotDurationPolicy::SnapshotDurationPolicy(TS_TYPE us) : us_(us) {
}

bool
SnapshotDurationPolicy::ShouldEject(const MapT& ids) {
    if (ids.size() <= 1) {
        return false;
    }
    bool should = true;
    auto ss = ids.begin()->second;
    auto now_us = GetMicroSecTimeStamp();
    if (now_us - ss->GetCollectionCommit()->GetCreatedTime() < us_) {
        should = false;
    }

    /* LOG_ENGINE_DEBUG_ << " now= " << now_us << " should=" << should; */
    /* LOG_ENGINE_DEBUG_ << "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV " << ids.size() << " xxxxx " << should; */
    /* for (auto it : ids) { */
    /*     LOG_ENGINE_DEBUG_ << " id=" << it.first << " ts=" << it.second->GetCollectionCommit()->GetCreatedTime(); */
    /* } */
    return should;
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
