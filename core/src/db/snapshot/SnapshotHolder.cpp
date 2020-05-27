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

ScopedSnapshotT
SnapshotHolder::GetSnapshot(ID_TYPE id, bool scoped) {
    if (id > max_id_) {
        auto entry = LoadNoLock(id);
        if (!entry)
            return ScopedSnapshotT();
        Add(id);
    }

    std::unique_lock<std::mutex> lock(mutex_);
    /* std::cout << "Holder " << collection_id_ << " actives num=" << active_.size() */
    /*     << " latest=" << active_[max_id_]->GetID() << " RefCnt=" << active_[max_id_]->RefCnt() <<  std::endl; */
    if (id == 0 || id == max_id_) {
        auto ss = active_[max_id_];
        return ScopedSnapshotT(ss, scoped);
    }
    if (id < min_id_) {
        return ScopedSnapshotT();
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        return ScopedSnapshotT();
    }
    return ScopedSnapshotT(it->second, scoped);
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
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (active_.size() > 0 && id < max_id_) {
            return Status(40007, "Invalid ID");
        }
        auto it = active_.find(id);
        if (it != active_.end()) {
            return Status(40008, "Duplicated ID");
        }
    }
    Snapshot::Ptr oldest_ss;
    {
        auto ss = std::make_shared<Snapshot>(id);

        std::unique_lock<std::mutex> lock(mutex_);
        if (!IsActive(ss)) {
            return Status(40009, "Specified collection is not active now");
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
            return Status::OK();

        auto oldest_it = active_.find(min_id_);
        oldest_ss = oldest_it->second;
        active_.erase(oldest_it);
        min_id_ = active_.begin()->first;
    }
    ReadyForRelease(oldest_ss);  // TODO: Use different mutex
    return Status::OK();
}

CollectionCommitPtr
SnapshotHolder::LoadNoLock(ID_TYPE collection_commit_id) {
    assert(collection_commit_id > max_id_);
    LoadOperationContext context;
    context.id = collection_commit_id;
    auto op = std::make_shared<LoadOperation<CollectionCommit>>(context);
    op->Push();
    return op->GetResource();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
