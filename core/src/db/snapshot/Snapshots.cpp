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

#include "config/ServerConfig.h"
#include "db/Constants.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/InActiveResourcesGCEvent.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/SnapshotPolicyFactory.h"
#include "utils/CommonUtil.h"
#include "utils/TimerContext.h"

#include <utility>

namespace milvus::engine::snapshot {

static constexpr int DEFAULT_READER_TIMER_INTERVAL_US = 60 * 1000;
static constexpr int DEFAULT_WRITER_TIMER_INTERVAL_US = 2000 * 1000;

Status
Snapshots::DropCollection(ID_TYPE collection_id, const LSN_TYPE& lsn) {
    ScopedSnapshotT ss;
    STATUS_CHECK(GetSnapshot(ss, collection_id));
    return DoDropCollection(ss, lsn);
}

Status
Snapshots::DropCollection(const std::string& name, const LSN_TYPE& lsn) {
    ScopedSnapshotT ss;
    STATUS_CHECK(GetSnapshot(ss, name));
    return DoDropCollection(ss, lsn);
}

Status
Snapshots::DoDropCollection(ScopedSnapshotT& ss, const LSN_TYPE& lsn) {
    OperationContext context;
    context.lsn = lsn;
    context.collection = ss->GetCollection();
    auto op = std::make_shared<DropCollectionOperation>(context, ss);
    op->Push();
    auto status = op->GetStatus();

    std::vector<SnapshotHolderPtr> holders;
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        alive_cids_.erase(context.collection->GetID());
        name_id_map_.erase(context.collection->GetName());
        /* holders_.erase(context.collection->GetID()); */
        auto h = holders_.find(context.collection->GetID());
        if (h != holders_.end()) {
            /* inactive_holders_[h->first] = h->second; */
            holders.push_back(h->second);
            holders_.erase(h);
        }
    }

    {
        std::unique_lock<std::shared_timed_mutex> lock(inactive_mtx_);
        for (auto& h : holders) {
            inactive_holders_[h->GetID()] = h;
        }
        holders.clear();
    }

    return status;
}

Status
Snapshots::DropPartition(const ID_TYPE& collection_id, const ID_TYPE& partition_id, const LSN_TYPE& lsn) {
    ScopedSnapshotT ss;
    STATUS_CHECK(GetSnapshot(ss, collection_id));

    PartitionContext context;
    context.id = partition_id;
    context.lsn = lsn;

    auto op = std::make_shared<DropPartitionOperation>(context, ss);
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetSnapshot(ss));

    return op->GetStatus();
}

Status
Snapshots::NumOfSnapshot(const std::string& collection_name, int& num) const {
    SnapshotHolderPtr holder;
    STATUS_CHECK(GetHolder(collection_name, holder));
    num = holder->NumOfSnapshot();
    return Status::OK();
}

Status
Snapshots::LoadSnapshot(StorePtr store, ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id, bool scoped) {
    SnapshotHolderPtr holder;
    STATUS_CHECK(LoadHolder(store, collection_id, holder));
    return holder->Load(store, ss, id, scoped);
}

Status
Snapshots::GetSnapshot(ScopedSnapshotT& ss, ID_TYPE collection_id, ID_TYPE id, bool scoped) const {
    SnapshotHolderPtr holder;
    STATUS_CHECK(GetHolder(collection_id, holder));
    return holder->Get(ss, id, scoped);
}

Status
Snapshots::GetSnapshot(ScopedSnapshotT& ss, const std::string& name, ID_TYPE id, bool scoped) const {
    SnapshotHolderPtr holder;
    STATUS_CHECK(GetHolder(name, holder));
    return holder->Get(ss, id, scoped);
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
Snapshots::GetCollectionNames(std::vector<std::string>& names) const {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto& kv : name_id_map_) {
        names.push_back(kv.first);
    }
    return Status::OK();
}

Status
Snapshots::LoadNoLock(StorePtr store, ID_TYPE collection_id, SnapshotHolderPtr& holder) {
    auto op = std::make_shared<GetSnapshotIDsOperation>(collection_id, false);
    /* op->Push(); */
    (*op)(store);
    auto& collection_commit_ids = op->GetIDs();
    if (collection_commit_ids.size() == 0) {
        std::stringstream emsg;
        emsg << "Snapshots::LoadNoLock: No collection commit is found for collection " << collection_id;
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }

    auto policy = SnapshotPolicyFactory::Build(config);
    holder = std::make_shared<SnapshotHolder>(collection_id, policy,
                                              std::bind(&Snapshots::SnapshotGCCallback, this, std::placeholders::_1));
    for (auto c_c_id : collection_commit_ids) {
        holder->Add(store, c_c_id);
    }
    return Status::OK();
}

Status
Snapshots::Init(StorePtr store) {
    auto event = std::make_shared<InActiveResourcesGCEvent>();
    EventExecutor::GetInstance().Submit(event, true);
    STATUS_CHECK(event->WaitToFinish());
    auto op = std::make_shared<GetCollectionIDsOperation>();
    STATUS_CHECK((*op)(store));
    auto& collection_ids = op->GetIDs();
    SnapshotHolderPtr holder;
    for (auto& collection_id : collection_ids) {
        STATUS_CHECK(LoadHolder(store, collection_id, holder));
    }
    return Status::OK();
}

Status
Snapshots::GetHolder(const std::string& name, SnapshotHolderPtr& holder) const {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto kv = name_id_map_.find(name);
    if (kv != name_id_map_.end()) {
        lock.unlock();
        return GetHolder(kv->second, holder);
    }
    std::stringstream emsg;
    emsg << "Snapshots::GetHolderNoLock: Specified snapshot holder for collection ";
    emsg << "\"" << name << "\""
         << " not found";
    LOG_SERVER_DEBUG_ << emsg.str();
    return Status(SS_NOT_FOUND_ERROR, "Collection " + name + " not found.");
}

Status
Snapshots::GetHolder(const ID_TYPE& collection_id, SnapshotHolderPtr& holder) const {
    Status status;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    status = GetHolderNoLock(collection_id, holder);

    return status;
}

Status
Snapshots::LoadHolder(StorePtr store, const ID_TYPE& collection_id, SnapshotHolderPtr& holder) {
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto status = GetHolderNoLock(collection_id, holder);
        if (status.ok() && holder) {
            return status;
        }
    }
    STATUS_CHECK(LoadNoLock(store, collection_id, holder));

    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_[collection_id] = holder;
    ScopedSnapshotT ss;
    STATUS_CHECK(holder->Load(store, ss));
    name_id_map_[ss->GetName()] = collection_id;
    alive_cids_.insert(collection_id);
    return Status::OK();
}

Status
Snapshots::GetHolderNoLock(ID_TYPE collection_id, SnapshotHolderPtr& holder) const {
    auto it = holders_.find(collection_id);
    if (it == holders_.end()) {
        std::stringstream emsg;
        emsg << "Snapshots::GetHolderNoLock: Specified snapshot holder for collection " << collection_id;
        emsg << " not found";
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }
    holder = it->second;
    return Status::OK();
}

void
Snapshots::OnReaderTimer(const boost::system::error_code& ec) {
    auto op = std::make_shared<GetAllActiveSnapshotIDsOperation>();
    auto status = (*op)(store_);
    if (!status.ok()) {
        LOG_SERVER_ERROR_ << "Snapshots::OnReaderTimer::GetAllActiveSnapshotIDsOperation failed: " << status.message();
        // TODO: Should be monitored
        return;
    }
    auto ids = op->GetIDs();
    ScopedSnapshotT ss;
    std::set<ID_TYPE> alive_cids;
    std::set<ID_TYPE> this_invalid_cids;
    bool diff_found = false;
    for (auto& [cid, ccid] : ids) {
        status = LoadSnapshot(store_, ss, cid, ccid);
        if (status.code() == SS_NOT_ACTIVE_ERROR) {
            auto found_it = invalid_ssid_.find(ccid);
            this_invalid_cids.insert(ccid);
            if (found_it == invalid_ssid_.end()) {
                LOG_SERVER_ERROR_ << status.ToString();
                diff_found = true;
            }
            continue;
        } else if (!status.ok()) {
            continue;
        }
        if (ss && ss->GetCollection()->IsActive()) {
            alive_cids.insert(cid);
        }
    }

    if (diff_found) {
        LOG_SERVER_ERROR_ << "Total " << this_invalid_cids.size() << " invalid SS found!";
    }

    if (invalid_ssid_.size() != 0 && (this_invalid_cids.size() == 0)) {
        LOG_SERVER_ERROR_ << "All invalid SS Cleared!";
        // TODO: Should be monitored
    }

    invalid_ssid_ = std::move(this_invalid_cids);
    auto op2 = std::make_shared<GetCollectionIDsOperation>();
    status = (*op2)(store_);
    if (!status.ok()) {
        LOG_SERVER_ERROR_ << "Snapshots::OnReaderTimer::GetCollectionIDsOperation failed: " << status.message();
        // TODO: Should be monitored
        return;
    }
    auto aids = op2->GetIDs();

    std::set<ID_TYPE> diff;
    std::set_difference(alive_cids_.begin(), alive_cids_.end(), aids.begin(), aids.end(),
                        std::inserter(diff, diff.begin()));
    for (auto& cid : diff) {
        ScopedSnapshotT ss;
        status = GetSnapshot(ss, cid);
        if (!status.ok()) {
            // TODO: Should not happen
            continue;
        }
        alive_cids_.erase(cid);
        name_id_map_.erase(ss->GetName());
        holders_.erase(cid);
    }
}

void
Snapshots::OnWriterTimer(const boost::system::error_code& ec) {
    // Single mode
    if (!config.cluster.enable()) {
        std::unique_lock<std::shared_timed_mutex> lock(inactive_mtx_);
        inactive_holders_.clear();
        return;
    }
    // Cluster RW mode
    std::unique_lock<std::shared_timed_mutex> lock(inactive_mtx_);
    auto it = inactive_holders_.cbegin();
    auto it_next = it;

    for (; it != inactive_holders_.cend(); it = it_next) {
        ++it_next;
        auto status = it->second->ApplyEject();
        if (status.code() == SS_EMPTY_HOLDER) {
            inactive_holders_.erase(it);
        }
    }
}

Status
Snapshots::RegisterTimers(TimerManager* mgr) {
    auto is_cluster = config.cluster.enable();
    auto role = config.cluster.role();
    if (is_cluster && (role == ClusterRole::RO)) {
        TimerContext::Context ctx;
        ctx.interval_us = DEFAULT_READER_TIMER_INTERVAL_US;
        ctx.handler = std::bind(&Snapshots::OnReaderTimer, this, std::placeholders::_1);
        mgr->AddTimer(ctx);
    } else {
        TimerContext::Context ctx;
        ctx.interval_us = DEFAULT_WRITER_TIMER_INTERVAL_US;
        ctx.handler = std::bind(&Snapshots::OnWriterTimer, this, std::placeholders::_1);
        mgr->AddTimer(ctx);
    }
    return Status::OK();
}

Status
Snapshots::Reset() {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_.clear();
    alive_cids_.clear();
    name_id_map_.clear();
    inactive_holders_.clear();
    return Status::OK();
}

void
Snapshots::SnapshotGCCallback(Snapshot::Ptr ss_ptr) {
    ss_ptr->UnRef();
    LOG_ENGINE_DEBUG_ << "Snapshot " << ss_ptr->GetID() << " ref_count = " << ss_ptr->ref_count() << " To be removed";
}

Status
Snapshots::StartService() {
    auto meta_path = config.storage.path() + DB_FOLDER;

    // create db root path
    auto s = CommonUtil::CreateDirectory(meta_path);
    if (!s.ok()) {
        std::cerr << "Error: Failed to create database primary path: " << meta_path
                  << ". Possible reason: db_config.primary_path is wrong in milvus.yaml or not available." << std::endl;
        kill(0, SIGUSR1);
    }

    store_ = snapshot::Store::Build(config.general.meta_uri(), meta_path, codec::Codec::instance().GetSuffixSet());
    snapshot::OperationExecutor::Init(store_);
    snapshot::OperationExecutor::GetInstance().Start();
    snapshot::EventExecutor::Init(store_);
    snapshot::EventExecutor::GetInstance().Start();
    return snapshot::Snapshots::GetInstance().Init(store_);
}

Status
Snapshots::StopService() {
    Reset();
    snapshot::EventExecutor::GetInstance().Stop();
    snapshot::OperationExecutor::GetInstance().Stop();
    return Status::OK();
}

}  // namespace milvus::engine::snapshot
