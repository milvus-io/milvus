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

class SnapshotPolicy {
 public:
    using MapT = std::map<ID_TYPE, Snapshot::Ptr>;

    virtual bool
    ShouldEject(const MapT& ids) = 0;

    virtual ~SnapshotPolicy() {
    }
};

using SnapshotPolicyPtr = std::shared_ptr<SnapshotPolicy>;

class SnapshotNumPolicy : public SnapshotPolicy {
 public:
    explicit SnapshotNumPolicy(size_t num);
    bool
    ShouldEject(const MapT& ids) override;

 protected:
    size_t num_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
