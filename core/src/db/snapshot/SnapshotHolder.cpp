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

#include <string>

namespace milvus {
namespace engine {
namespace snapshot {

/* SnapshotHolder::SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler, size_t num_versions) */
/*     : collection_id_(collection_id), num_versions_(num_versions), gc_handler_(gc_handler) { */
/* } */
SnapshotHolder::SnapshotHolder(ID_TYPE collection_id, SnapshotPolicyPtr policy, GCHandler gc_handler)
    : collection_id_(collection_id), policy_(policy), gc_handler_(gc_handler) {
}

SnapshotHolder::~SnapshotHolder() {
    bool release = false;
    for (auto& [_, ss] : active_) {
        if (!ss->GetCollection()->IsActive()) {
            ReadyForRelease(ss);
            release = true;
        }
    }

    if (release) {
        active_.clear();
    }
}

Status
SnapshotHolder::Load(StorePtr store, ScopedSnapshotT& ss, ID_TYPE id, bool scoped) {
    if (id > max_id_) {
        CollectionCommitPtr cc;
        STATUS_CHECK(LoadNoLock(id, cc, store));
        STATUS_CHECK(Add(store, id));
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (id == 0 || id == max_id_) {
        auto raw = active_.at(max_id_);
        ss = ScopedSnapshotT(raw, scoped);
        return Status::OK();
    }
    if (id < min_id_) {
        std::stringstream emsg;
        emsg << "SnapshotHolder::Load: Got stale snapshot " << id;
        emsg << " current is " << max_id_;
        emsg << " on collection " << collection_id_;
        return Status(SS_STALE_ERROR, emsg.str());
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        std::stringstream emsg;
        emsg << "SnapshotHolder::Load: Specified snapshot " << id << " not found.";
        emsg << " Current is " << max_id_;
        emsg << " on collection " << collection_id_;
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }
    ss = ScopedSnapshotT(it->second, scoped);
    return Status::OK();
}

Status
SnapshotHolder::Get(ScopedSnapshotT& ss, ID_TYPE id, bool scoped) const {
    Status status;
    if (id > max_id_) {
        std::stringstream emsg;
        emsg << "SnapshotHolder::Get: Specified snapshot " << id << " not found.";
        emsg << " Current is " << max_id_;
        emsg << " on collection " << collection_id_;
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (id == 0 || id == max_id_) {
        auto raw = active_.at(max_id_);
        ss = ScopedSnapshotT(raw, scoped);
        return status;
    }
    if (id < min_id_) {
        std::stringstream emsg;
        emsg << "SnapshotHolder::Get: Got stale snapshot " << id;
        emsg << " current is " << max_id_;
        emsg << " on collection " << collection_id_;
        return Status(SS_STALE_ERROR, emsg.str());
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        std::stringstream emsg;
        emsg << "SnapshotHolder::Get: Specified snapshot " << id << " not found.";
        emsg << " Current is " << max_id_;
        emsg << " on collection " << collection_id_;
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }
    ss = ScopedSnapshotT(it->second, scoped);
    return status;
}

int
SnapshotHolder::NumOfSnapshot() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return active_.size();
}

bool
SnapshotHolder::IsActive(Snapshot::Ptr& ss) {
    auto collection = ss->GetCollection();
    return collection && collection->IsActive();
}

Status
SnapshotHolder::ApplyEject() {
    Status status;
    Snapshot::Ptr oldest_ss;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (active_.size() == 0) {
            return Status(SS_EMPTY_HOLDER,
                          "SnapshotHolder::ApplyEject: Empty holder found for " + std::to_string(collection_id_));
        }
        if (!policy_->ShouldEject(active_, false)) {
            return status;
        }
        auto oldest_it = active_.find(min_id_);
        oldest_ss = oldest_it->second;
        active_.erase(oldest_it);
        if (active_.size() > 0) {
            min_id_ = active_.begin()->first;
        }
    }
    ReadyForRelease(oldest_ss);
    return status;
}

Status
SnapshotHolder::Add(StorePtr store, ID_TYPE id) {
    Status status;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (active_.size() > 0 && id < max_id_) {
            std::stringstream emsg;
            emsg << "SnapshotHolder::Add: Invalid snapshot " << id << ".";
            emsg << " Should larger than " << max_id_;
            return Status(SS_INVALID_ARGUMENT_ERROR, emsg.str());
        }
        auto it = active_.find(id);
        if (it != active_.end()) {
            std::stringstream emsg;
            emsg << "SnapshotHolder::Add: Duplicated snapshot " << id << ".";
            return Status(SS_DUPLICATED_ERROR, emsg.str());
        }
    }
    Snapshot::Ptr oldest_ss;
    {
        auto ss = std::make_shared<Snapshot>(store, id);
        if (!ss->IsValid()) {
            std::string emsg = "SnapshotHolder::Add: Invalid SS " + std::to_string(id);
            return Status(SS_NOT_ACTIVE_ERROR, emsg);
        }

        std::unique_lock<std::mutex> lock(mutex_);
        if (!IsActive(ss)) {
            std::stringstream emsg;
            emsg << "SnapshotHolder::Add: Specified collection " << collection_id_;
            return Status(SS_NOT_ACTIVE_ERROR, emsg.str());
        }
        // TODO(cqx): Do not use std::bind which cause ss destruct problem with 1 reference count.
        // ss->RegisterOnNoRefCB(std::bind(&Snapshot::UnRefAll, std::ref(ss)));
        ss->Ref();

        if (min_id_ > id) {
            min_id_ = id;
        }

        if (max_id_ < id) {
            max_id_ = id;
        }

        active_[id] = ss;
        /* if (active_.size() <= num_versions_) { */
        /*     return status; */
        /* } */
        if (!policy_->ShouldEject(active_)) {
            return status;
        }

        auto oldest_it = active_.find(min_id_);
        oldest_ss = oldest_it->second;
        active_.erase(oldest_it);
        min_id_ = active_.begin()->first;
    }
    ReadyForRelease(oldest_ss);
    return status;
}

Status
SnapshotHolder::LoadNoLock(ID_TYPE collection_commit_id, CollectionCommitPtr& cc, StorePtr store) {
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
