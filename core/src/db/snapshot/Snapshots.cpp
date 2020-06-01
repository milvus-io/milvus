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

#include "db/snapshot/Snapshots.h"
#include "db/snapshot/CompoundOperations.h"

namespace milvus {
namespace engine {
namespace snapshot {

Status
Snapshots::DropCollection(ID_TYPE collection_id) {
    ScopedSnapshotT ss;
    auto status = GetSnapshot(ss, collection_id);
    if (!status.ok())
        return status;
    return DoDropCollection(ss);
}

Status
Snapshots::DropCollection(const std::string& name) {
    ScopedSnapshotT ss;
    auto status = GetSnapshot(ss, name);
    if (!status.ok())
        return status;
    return DoDropCollection(ss);
}

Status
Snapshots::DoDropCollection(ScopedSnapshotT& ss) {
    OperationContext context;
    context.collection = ss->GetCollection();
    auto op = std::make_shared<SoftDeleteCollectionOperation>(context);
    op->Push();
    auto status = op->GetStatus();

    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    name_id_map_.erase(context.collection->GetName());
    holders_.erase(context.collection->GetID());
    return status;
}

Status
Snapshots::GetSnapshotNoLoad(ScopedSnapshotT& ss, ID_TYPE collection_id, bool scoped) {
    SnapshotHolderPtr holder;
    auto status = GetHolder(collection_id, holder, false);
    if (!status.ok())
        return status;
    status = holder->GetSnapshot(ss, 0, scoped, false);
    return status;
}

Status
Snapshots::GetSnapshot(ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id, bool scoped) {
    SnapshotHolderPtr holder;
    auto status = GetHolder(collection_id, holder);
    if (!status.ok())
        return status;
    status = holder->GetSnapshot(ss, id, scoped);
    return status;
}

Status
Snapshots::GetSnapshot(ScopedSnapshotT& ss, const std::string& name, ID_TYPE id, bool scoped) {
    SnapshotHolderPtr holder;
    auto status = GetHolder(name, holder);
    if (!status.ok())
        return status;
    status = holder->GetSnapshot(ss, id, scoped);
    return status;
}

Status
Snapshots::GetCollectionIds(IDS_TYPE& ids) const {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto& kv : holders_) {
        ids.push_back(kv.first);
    }
    return Status::OK();
}

Status
Snapshots::LoadNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder) {
    auto op = std::make_shared<GetSnapshotIDsOperation>(collection_id, false);
    op->Push();
    auto& collection_commit_ids = op->GetIDs();
    if (collection_commit_ids.size() == 0) {
        return Status(SS_NOT_FOUND_ERROR, "No collection commit found");
    }
    holder = std::make_shared<SnapshotHolder>(collection_id,
                                              std::bind(&Snapshots::SnapshotGCCallback, this, std::placeholders::_1));
    for (auto c_c_id : collection_commit_ids) {
        holder->Add(c_c_id);
    }
    return Status::OK();
}

void
Snapshots::Init() {
    auto op = std::make_shared<GetCollectionIDsOperation>();
    op->Push();
    auto& collection_ids = op->GetIDs();
    SnapshotHolderPtr holder;
    for (auto collection_id : collection_ids) {
        GetHolder(collection_id, holder);
    }
}

Status
Snapshots::GetHolder(const std::string& name, SnapshotHolderPtr& holder) {
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto kv = name_id_map_.find(name);
        if (kv != name_id_map_.end()) {
            lock.unlock();
            return GetHolder(kv->second, holder);
        }
    }
    LoadOperationContext context;
    context.name = name;
    auto op = std::make_shared<LoadOperation<Collection>>(context);
    op->Push();
    CollectionPtr c;
    auto status = op->GetResource(c);
    if (!status.ok())
        return status;
    return GetHolder(c->GetID(), holder);
}

Status
Snapshots::GetHolder(ID_TYPE collection_id, SnapshotHolderPtr& holder, bool load) {
    Status status;
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        status = GetHolderNoLock(collection_id, holder);
        if (status.ok() && holder)
            return status;
        if (!load)
            return status;
    }
    status = LoadNoLock(collection_id, holder);
    if (!status.ok())
        return status;

    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_[collection_id] = holder;
    ScopedSnapshotT ss;
    status = holder->GetSnapshot(ss);
    if (!status.ok())
        return status;
    name_id_map_[ss->GetName()] = collection_id;
    return status;
}

Status
Snapshots::GetHolderNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder) {
    auto it = holders_.find(collection_id);
    if (it == holders_.end()) {
        return Status(SS_NOT_FOUND_ERROR, "Specified snapshot holder not found");
    }
    holder = it->second;
    return Status::OK();
}

Status
Snapshots::Reset() {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_.clear();
    name_id_map_.clear();
    to_release_.clear();
    return Status::OK();
}

void
Snapshots::SnapshotGCCallback(Snapshot::Ptr ss_ptr) {
    /* to_release_.push_back(ss_ptr); */
    ss_ptr->UnRef();
    std::cout << &(*ss_ptr) << " Snapshot " << ss_ptr->GetID() << " RefCnt = " << ss_ptr->RefCnt() << " To be removed"
              << std::endl;
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
