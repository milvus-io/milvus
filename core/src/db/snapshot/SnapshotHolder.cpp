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

#include "db/snapshot/SnapshotHolder.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceHolders.h"

namespace milvus {
namespace engine {
namespace snapshot {

SnapshotHolder::SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler, size_t num_versions)
    : collection_id_(collection_id), num_versions_(num_versions), gc_handler_(gc_handler) {
}

SnapshotHolder::~SnapshotHolder() {
    bool release = false;
    for (auto& ss_kv : active_) {
        if (!ss_kv.second->GetCollection()->IsActive()) {
            ReadyForRelease(ss_kv.second);
            release = true;
        }
    }

    if (release) {
        active_.clear();
    }
}

Status
SnapshotHolder::Load(Store& store, ScopedSnapshotT& ss, ID_TYPE id, bool scoped) {
    Status status;
    if (id > max_id_) {
        CollectionCommitPtr cc;
        status = LoadNoLock(id, cc, store);
        if (!status.ok())
            return status;
        status = Add(id);
        if (!status.ok())
            return status;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (id == 0 || id == max_id_) {
        auto raw = active_[max_id_];
        ss = ScopedSnapshotT(raw, scoped);
        return status;
    }
    if (id < min_id_) {
        return Status(SS_STALE_ERROR, "Get stale snapshot");
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        return Status(SS_NOT_FOUND_ERROR, "Specified Snapshot not found");
    }
    ss = ScopedSnapshotT(it->second, scoped);
    return status;
}

Status
SnapshotHolder::Get(ScopedSnapshotT& ss, ID_TYPE id, bool scoped) {
    Status status;
    if (id > max_id_) {
        return Status(SS_NOT_FOUND_ERROR, "Specified Snapshot not found");
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (id == 0 || id == max_id_) {
        auto raw = active_[max_id_];
        ss = ScopedSnapshotT(raw, scoped);
        return status;
    }
    if (id < min_id_) {
        return Status(SS_STALE_ERROR, "Get stale snapshot");
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        return Status(SS_NOT_FOUND_ERROR, "Specified Snapshot not found");
    }
    ss = ScopedSnapshotT(it->second, scoped);
    return status;
}

bool
SnapshotHolder::IsActive(Snapshot::Ptr& ss) {
    auto collection = ss->GetCollection();
    if (collection && collection->IsActive()) {
        return true;
    }
    return false;
}

Status
SnapshotHolder::Add(ID_TYPE id) {
    Status status;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (active_.size() > 0 && id < max_id_) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid ID");
        }
        auto it = active_.find(id);
        if (it != active_.end()) {
            return Status(SS_DUPLICATED_ERROR, "Duplicated ID");
        }
    }
    Snapshot::Ptr oldest_ss;
    {
        auto ss = std::make_shared<Snapshot>(id);

        std::unique_lock<std::mutex> lock(mutex_);
        if (!IsActive(ss)) {
            return Status(SS_NOT_ACTIVE_ERROR, "Specified collection is not active now");
        }
        ss->RegisterOnNoRefCB(std::bind(&Snapshot::UnRefAll, ss));
        ss->Ref();

        if (min_id_ > id) {
            min_id_ = id;
        }

        if (max_id_ < id) {
            max_id_ = id;
        }

        active_[id] = ss;
        if (active_.size() <= num_versions_)
            return status;

        auto oldest_it = active_.find(min_id_);
        oldest_ss = oldest_it->second;
        active_.erase(oldest_it);
        min_id_ = active_.begin()->first;
    }
    ReadyForRelease(oldest_ss);
    return status;
}

Status
SnapshotHolder::LoadNoLock(ID_TYPE collection_commit_id, CollectionCommitPtr& cc, Store& store) {
    assert(collection_commit_id > max_id_);
    LoadOperationContext context;
    context.id = collection_commit_id;
    auto op = std::make_shared<LoadOperation<CollectionCommit>>(context);
    (*op)(store);
    return op->GetResource(cc);
}

/* Status */
/* SnapshotHolder::LoadNoLock(ID_TYPE collection_commit_id, CollectionCommitPtr& cc) { */
/*     assert(collection_commit_id > max_id_); */
/*     LoadOperationContext context; */
/*     context.id = collection_commit_id; */
/*     auto op = std::make_shared<LoadOperation<CollectionCommit>>(context); */
/*     op->Push(); */
/*     return op->GetResource(cc); */
/* } */

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
