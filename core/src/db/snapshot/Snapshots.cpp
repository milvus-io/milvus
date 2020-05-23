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

template <typename... ResourceT>
bool
Snapshots::Flush(ResourceT&&... resources) {
    auto t = std::make_tuple(resources...);
    std::apply([](auto&&... args) { ((std::cout << args << "\n"), ...); }, t);
    return true;
}

bool
Snapshots::DropCollection(const std::string& name) {
    std::map<std::string, ID_TYPE>::iterator it;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        it = name_id_map_.find(name);
        if (it == name_id_map_.end()) {
            return false;
        }
    }

    return true;
}

ScopedSnapshotT
Snapshots::GetSnapshot(ID_TYPE collection_id, ID_TYPE id, bool scoped) {
    auto holder = GetHolder(collection_id);
    if (!holder)
        return ScopedSnapshotT();
    return holder->GetSnapshot(id, scoped);
}

ScopedSnapshotT
Snapshots::GetSnapshot(const std::string& name, ID_TYPE id, bool scoped) {
    auto holder = GetHolder(name);
    if (!holder)
        return ScopedSnapshotT();
    return holder->GetSnapshot(id, scoped);
}

IDS_TYPE
Snapshots::GetCollectionIds() const {
    IDS_TYPE ids;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto& kv : holders_) {
        ids.push_back(kv.first);
    }
    return ids;
}

bool
Snapshots::Close(ID_TYPE collection_id) {
    auto ss = GetSnapshot(collection_id);
    if (!ss)
        return false;
    auto name = ss->GetName();
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_.erase(collection_id);
    name_id_map_.erase(name);
    return true;
}

/* SnapshotHolderPtr */
/* Snapshots::Load(ID_TYPE collection_id) { */
/*     std::unique_lock<std::shared_timed_mutex> lock(mutex_); */
/*     return LoadNoLock(collection_id); */
/* } */

SnapshotHolderPtr
Snapshots::LoadNoLock(ID_TYPE collection_id) {
    auto op = std::make_shared<GetSnapshotIDsOperation>(collection_id, false);
    op->Push();
    auto& collection_commit_ids = op->GetIDs();
    if (collection_commit_ids.size() == 0) {
        return nullptr;
    }
    auto holder = std::make_shared<SnapshotHolder>(
        collection_id, std::bind(&Snapshots::SnapshotGCCallback, this, std::placeholders::_1));
    for (auto c_c_id : collection_commit_ids) {
        holder->Add(c_c_id);
    }
    return holder;
}

void
Snapshots::Init() {
    auto op = std::make_shared<GetCollectionIDsOperation>();
    op->Push();
    auto& collection_ids = op->GetIDs();
    for (auto collection_id : collection_ids) {
        GetHolder(collection_id);
    }
}

SnapshotHolderPtr
Snapshots::GetHolder(const std::string& name) {
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto kv = name_id_map_.find(name);
        if (kv != name_id_map_.end()) {
            lock.unlock();
            return GetHolder(kv->second);
        }
    }
    LoadOperationContext context;
    context.name = name;
    auto op = std::make_shared<LoadOperation<Collection>>(context);
    op->Push();
    auto c = op->GetResource();
    if (!c)
        return nullptr;
    return GetHolder(c->GetID());
}

SnapshotHolderPtr
Snapshots::GetHolder(ID_TYPE collection_id) {
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto holder = GetHolderNoLock(collection_id);
        if (holder)
            return holder;
    }
    auto holder = LoadNoLock(collection_id);
    if (!holder)
        return nullptr;

    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_[collection_id] = holder;
    name_id_map_[holder->GetSnapshot()->GetName()] = collection_id;
    return holder;
}

SnapshotHolderPtr
Snapshots::GetHolderNoLock(ID_TYPE collection_id) {
    auto it = holders_.find(collection_id);
    if (it == holders_.end()) {
        return nullptr;
    }
    return it->second;
}

void
Snapshots::Reset() {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_.clear();
    name_id_map_.clear();
    to_release_.clear();
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
