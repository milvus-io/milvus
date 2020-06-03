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

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include "db/snapshot/SnapshotHolder.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

class Snapshots {
 public:
    static Snapshots&
    GetInstance() {
        static Snapshots sss;
        return sss;
    }
    Status
    GetHolder(ID_TYPE collection_id, SnapshotHolderPtr& holder, bool load = true);
    Status
    GetHolder(const std::string& name, SnapshotHolderPtr& holder);

    Status
    GetSnapshot(ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id = 0, bool scoped = true);
    Status
    GetSnapshot(ScopedSnapshotT& ss, const std::string& name, ID_TYPE id = 0, bool scoped = true);
    Status
    GetSnapshotNoLoad(ScopedSnapshotT& ss, ID_TYPE collection_id, bool scoped = true);

    Status
    GetCollectionIds(IDS_TYPE& ids) const;

    Status
    DropCollection(const std::string& name);
    Status
    DropCollection(ID_TYPE collection_id);

    Status
    Reset();

 private:
    void
    SnapshotGCCallback(Snapshot::Ptr ss_ptr);
    Snapshots() {
        Init();
    }
    Status
    DoDropCollection(ScopedSnapshotT& ss);
    void
    Init();

    Status
    LoadNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder);
    Status
    GetHolderNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder);

    mutable std::shared_timed_mutex mutex_;
    std::map<ID_TYPE, SnapshotHolderPtr> holders_;
    std::map<std::string, ID_TYPE> name_id_map_;
    std::vector<Snapshot::Ptr> to_release_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
