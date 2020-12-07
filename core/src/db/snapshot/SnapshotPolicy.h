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
#include "db/snapshot/Snapshot.h"

namespace milvus {
namespace engine {
namespace snapshot {

// A base class for snapshot policy
class SnapshotPolicy {
 public:
    using MapT = std::map<ID_TYPE, Snapshot::Ptr>;

    // Check if should eject any snapshot in ids
    virtual bool
    ShouldEject(const MapT& ids, bool alive = true) = 0;

    virtual ~SnapshotPolicy() {
    }
};

using SnapshotPolicyPtr = std::shared_ptr<SnapshotPolicy>;

// A policy that keeps upto specified num of snapshots
class SnapshotNumPolicy : public SnapshotPolicy {
 public:
    explicit SnapshotNumPolicy(size_t num);

    bool
    ShouldEject(const MapT& ids, bool alive = true) override;

 protected:
    // Num of snapshots
    size_t num_;
};

// A policy that keeps all snapshots within specified duration
class SnapshotDurationPolicy : public SnapshotPolicy {
 public:
    explicit SnapshotDurationPolicy(TS_TYPE us);

    bool
    ShouldEject(const MapT& ids, bool alive = true) override;

 protected:
    // Duration in us
    TS_TYPE us_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
