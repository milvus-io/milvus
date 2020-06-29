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

#include "db/SSDBImpl.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Snapshots.h"
#include "wal/WalDefinations.h"

namespace milvus {
namespace engine {

namespace {
static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");
}  // namespace

#define CHECK_INITIALIZED \
    if (!initialized_.load(std::memory_order_acquire)) { \
        return SHUTDOWN_ERROR;   \
    }

SSDBImpl::SSDBImpl(const DBOptions& options) : options_(options), initialized_(false) {
    if (options_.wal_enable_) {
        wal::MXLogConfiguration mxlog_config;
        mxlog_config.recovery_error_ignore = options_.recovery_error_ignore_;
        // 2 buffers in the WAL
        mxlog_config.buffer_size = options_.buffer_size_ / 2;
        mxlog_config.mxlog_path = options_.mxlog_path_;
        wal_mgr_ = std::make_shared<wal::WalManager>(mxlog_config);
    }
    Start();
}

SSDBImpl::~SSDBImpl() {
    Stop();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// external api
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
SSDBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    return Status::OK();
}

Status
SSDBImpl::Stop() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    initialized_.store(false, std::memory_order_release);

    // LOG_ENGINE_TRACE_ << "DB service stop";
    return Status::OK();
}

Status
SSDBImpl::CreateCollection(const snapshot::CreateCollectionContext& context) {
    CHECK_INITIALIZED;

    auto ctx = context;

    if (options_.wal_enable_) {
        ctx.lsn = wal_mgr_->CreateCollection(context.collection->GetName());
    }

    auto op = std::make_shared<snapshot::CreateCollectionOperation>(ctx);
    auto status = op->Push();

    return status;
}

Status
SSDBImpl::DescribeCollection(const std::string& collection_name, snapshot::CollectionPtr& collection,
                                 std::map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    collection = ss->GetCollection();

    auto& fields = ss->GetResources<snapshot::Field>();
    for (auto& kv : fields) {
        fields_schema[kv.second.Get()] = ss->GetFieldElementsByField(kv.second->GetName());
    }
    return status;
}

Status
SSDBImpl::DropCollection(const std::string& name) {
    CHECK_INITIALIZED;

    // dates partly delete files of the collection but currently we don't support
    LOG_ENGINE_DEBUG_ << "Prepare to delete collection " << name;

    snapshot::ScopedSnapshotT ss;
    auto& snapshots = snapshot::Snapshots::GetInstance();
    auto status = snapshots.GetSnapshot(ss, name);
    if (!status.ok()) {
        return status;
    }

    if (options_.wal_enable_) {
        // SS TODO
        /* wal_mgr_->DropCollection(ss->GetCollectionId()); */
    }

    status = snapshots.DropCollection(ss->GetCollectionId(), std::numeric_limits<snapshot::LSN_TYPE>::max());
    return status;
}

Status
SSDBImpl::HasCollection(const std::string& collection_name, bool& has_or_not) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    has_or_not = status.ok();

    return status;
}

Status
SSDBImpl::AllCollections(std::vector<std::string>& names) {
    CHECK_INITIALIZED;

    names.clear();
    return snapshot::Snapshots::GetInstance().GetCollectionNames(names);
}

Status
SSDBImpl::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    uint64_t lsn = 0;
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    if (options_.wal_enable_) {
        // SS TODO
        /* lsn = wal_mgr_->CreatePartition(collection_id, partition_tag); */
    } else {
        lsn = ss->GetCollection()->GetLsn();
    }

    snapshot::OperationContext context;
    context.lsn = lsn;
    auto op = std::make_shared<snapshot::CreatePartitionOperation>(context, ss);

    snapshot::PartitionContext p_ctx;
    p_ctx.name = partition_name;
    snapshot::PartitionPtr partition;
    status = op->CommitNewPartition(p_ctx, partition);
    if (!status.ok()) {
        return status;
    }

    status = op->Push();
    return status;
}

Status
SSDBImpl::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    // SS TODO: Is below step needed? Or How to implement it?
    /* mem_mgr_->EraseMemVector(partition_name); */

    snapshot::PartitionContext context;
    context.name = partition_name;
    auto op = std::make_shared<snapshot::DropPartitionOperation>(context, ss);
    status = op->Push();

    return status;
}

Status
SSDBImpl::ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    partition_names = std::move(ss->GetPartitionNames());
    return status;
}

Status
SSDBImpl::PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                                bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    auto handler = std::make_shared<LoadVectorFieldHandler>(context, ss);
    handler->Iterate();

    return handler->GetStatus();
}

}  // namespace engine
}  // namespace milvus
