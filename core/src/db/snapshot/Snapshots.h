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
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include "db/snapshot/SnapshotHolder.h"
#include "db/snapshot/Store.h"
#include "utils/Status.h"
#include "utils/ThreadPool.h"
#include "utils/TimerManager.h"

namespace milvus::engine::snapshot {

class Snapshots {
 public:
    static Snapshots&
    GetInstance() {
        static Snapshots sss;
        return sss;
    }
    Status
    GetHolder(const ID_TYPE& collection_id, SnapshotHolderPtr& holder) const;
    Status
    GetHolder(const std::string& name, SnapshotHolderPtr& holder) const;
    Status
    LoadHolder(StorePtr store, const ID_TYPE& collection_id, SnapshotHolderPtr& holder);

    Status
    GetSnapshot(ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id = 0, bool scoped = true) const;
    Status
    GetSnapshot(ScopedSnapshotT& ss, const std::string& name, ID_TYPE id = 0, bool scoped = true) const;
    Status
    LoadSnapshot(StorePtr store, ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id, bool scoped = true);

    Status
    GetCollectionIds(IDS_TYPE& ids) const;
    Status
    GetCollectionNames(std::vector<std::string>& names) const;

    Status
    DropCollection(const std::string& name, const LSN_TYPE& lsn);
    Status
    DropCollection(ID_TYPE collection_id, const LSN_TYPE& lsn);

    Status
    DropPartition(const ID_TYPE& collection_id, const ID_TYPE& partition_id, const LSN_TYPE& lsn);

    Status
    NumOfSnapshot(const std::string& collection_name, int& num) const;

    Status
    Reset();

    Status Init(StorePtr);

    Status
    RegisterTimers(TimerManager* mgr);

 public:
    Status
    StartService();

    Status
    StopService();

 private:
    void
    SnapshotGCCallback(Snapshot::Ptr ss_ptr);
    Snapshots() = default;
    Status
    DoDropCollection(ScopedSnapshotT& ss, const LSN_TYPE& lsn);

    void
    OnReaderTimer(const boost::system::error_code&);
    void
    OnWriterTimer(const boost::system::error_code&);

    Status
    LoadNoLock(StorePtr store, ID_TYPE collection_id, SnapshotHolderPtr& holder);
    Status
    GetHolderNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder) const;

    mutable std::shared_timed_mutex mutex_;
    std::map<ID_TYPE, SnapshotHolderPtr> holders_;
    std::set<ID_TYPE> alive_cids_;
    std::map<std::string, ID_TYPE> name_id_map_;
    mutable std::shared_timed_mutex inactive_mtx_;
    std::map<ID_TYPE, SnapshotHolderPtr> inactive_holders_;
    std::set<ID_TYPE> invalid_ssid_;
    StorePtr store_;
};

}  // namespace milvus::engine::snapshot
