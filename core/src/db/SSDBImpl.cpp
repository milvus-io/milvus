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
#include "cache/CpuCacheMgr.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshots.h"
#include "insert/MemManagerFactory.h"
#include "metrics/Metrics.h"
#include "metrics/SystemInfo.h"
#include "utils/Exception.h"
#include "wal/WalDefinations.h"

#include <fiu-local.h>
#include <limits>
#include <utility>

namespace milvus {
namespace engine {

namespace {
constexpr uint64_t BACKGROUND_METRIC_INTERVAL = 1;
constexpr uint64_t BACKGROUND_INDEX_INTERVAL = 1;
constexpr uint64_t WAIT_BUILD_INDEX_INTERVAL = 5;

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");
}  // namespace

#define CHECK_INITIALIZED                                \
    if (!initialized_.load(std::memory_order_acquire)) { \
        return SHUTDOWN_ERROR;                           \
    }

SSDBImpl::SSDBImpl(const DBOptions& options)
    : options_(options), initialized_(false), merge_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    mem_mgr_ = MemManagerFactory::SSBuild(options_);

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

////////////////////////////////////////////////////////////////////////////////
// External APIs
////////////////////////////////////////////////////////////////////////////////
Status
SSDBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    // TODO: merge files

    // wal
    if (options_.wal_enable_) {
        auto error_code = DB_ERROR;
        if (wal_mgr_ != nullptr) {
            error_code = wal_mgr_->Init();
        }
        if (error_code != WAL_SUCCESS) {
            throw Exception(error_code, "Wal init error!");
        }

        // recovery
        while (1) {
            wal::MXLogRecord record;
            auto error_code = wal_mgr_->GetNextRecovery(record);
            if (error_code != WAL_SUCCESS) {
                throw Exception(error_code, "Wal recovery error!");
            }
            if (record.type == wal::MXLogType::None) {
                break;
            }
            ExecWalRecord(record);
        }

        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background wal thread
            bg_wal_thread_ = std::thread(&SSDBImpl::BackgroundWalThread, this);
        }
    } else {
        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background flush thread
            bg_flush_thread_ = std::thread(&SSDBImpl::BackgroundFlushThread, this);
        }
    }

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background build index thread
        bg_index_thread_ = std::thread(&SSDBImpl::BackgroundIndexThread, this);
    }

    // background metric thread
    fiu_do_on("options_metric_enable", options_.metric_enable_ = true);
    if (options_.metric_enable_) {
        bg_metric_thread_ = std::thread(&SSDBImpl::BackgroundMetricThread, this);
    }

    return Status::OK();
}

Status
SSDBImpl::Stop() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    initialized_.store(false, std::memory_order_release);

    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        if (options_.wal_enable_) {
            // wait wal thread finish
            swn_wal_.Notify();
            bg_wal_thread_.join();
        } else {
            // flush all without merge
            wal::MXLogRecord record;
            record.type = wal::MXLogType::Flush;
            ExecWalRecord(record);

            // wait flush thread finish
            swn_flush_.Notify();
            bg_flush_thread_.join();
        }

        WaitMergeFileFinish();

        swn_index_.Notify();
        bg_index_thread_.join();
    }

    // wait metric thread exit
    if (options_.metric_enable_) {
        swn_metric_.Notify();
        bg_metric_thread_.join();
    }

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
    return op->Push();
}

Status
SSDBImpl::DescribeCollection(const std::string& collection_name, snapshot::CollectionPtr& collection,
                             std::map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    collection = ss->GetCollection();
    auto& fields = ss->GetResources<snapshot::Field>();
    for (auto& kv : fields) {
        fields_schema[kv.second.Get()] = ss->GetFieldElementsByField(kv.second->GetName());
    }
    return Status::OK();
}

Status
SSDBImpl::DropCollection(const std::string& name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Prepare to delete collection " << name;

    snapshot::ScopedSnapshotT ss;
    auto& snapshots = snapshot::Snapshots::GetInstance();
    STATUS_CHECK(snapshots.GetSnapshot(ss, name));

    if (options_.wal_enable_) {
        // SS TODO
        /* wal_mgr_->DropCollection(ss->GetCollectionId()); */
    }

    auto status = mem_mgr_->EraseMemVector(ss->GetCollectionId());  // not allow insert

    return snapshots.DropCollection(ss->GetCollectionId(), std::numeric_limits<snapshot::LSN_TYPE>::max());
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
SSDBImpl::GetCollectionRowCount(const std::string& collection_name, uint64_t& row_count) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    row_count = ss->GetCollectionCommit()->GetRowCount();
    return Status::OK();
}

Status
SSDBImpl::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    snapshot::LSN_TYPE lsn = 0;
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
    STATUS_CHECK(op->CommitNewPartition(p_ctx, partition));
    return op->Push();
}

Status
SSDBImpl::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    // SS TODO: Is below step needed? Or How to implement it?
    /* mem_mgr_->EraseMemVector(partition_name); */

    snapshot::PartitionContext context;
    context.name = partition_name;
    auto op = std::make_shared<snapshot::DropPartitionOperation>(context, ss);
    return op->Push();
}

Status
SSDBImpl::ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    partition_names = std::move(ss->GetPartitionNames());
    return Status::OK();
}

Status
SSDBImpl::DropIndex(const std::string& collection_name, const std::string& field_name,
                    const std::string& field_element_name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_name;
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    // SS TODO: Check Index Type

    snapshot::OperationContext context;
    // SS TODO: no lsn for drop index
    context.lsn = ss->GetCollectionCommit()->GetLsn();
    STATUS_CHECK(ss->GetFieldElement(field_name, field_element_name, context.stale_field_element));
    auto op = std::make_shared<snapshot::DropAllIndexOperation>(context, ss);
    STATUS_CHECK(op->Push());

    // SS TODO: Start merge task needed?
    /* std::set<std::string> merge_collection_ids = {collection_id}; */
    /* StartMergeTask(merge_collection_ids, true); */
    return Status::OK();
}

Status
SSDBImpl::PreloadCollection(const server::ContextPtr& context, const std::string& collection_name, bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<LoadVectorFieldHandler>(context, ss);
    handler->Iterate();

    return handler->GetStatus();
}

Status
SSDBImpl::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                        const std::vector<std::string>& field_names, std::vector<VectorsData>& vector_data,
                        std::vector<meta::hybrid::DataType>& attr_type, std::vector<AttrsData>& attr_data) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<GetEntityByIdSegmentHandler>(nullptr, ss, id_array, field_names);
    handler->Iterate();
    STATUS_CHECK(handler->GetStatus());

    vector_data = std::move(handler->vector_data_);
    attr_type = std::move(handler->attr_type_);
    attr_data = std::move(handler->attr_data_);

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// Internal APIs
////////////////////////////////////////////////////////////////////////////////
void
SSDBImpl::InternalFlush(const std::string& collection_id) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_id;
    ExecWalRecord(record);
}

void
SSDBImpl::BackgroundFlushThread() {
    SetThreadName("flush_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background flush thread exit";
            break;
        }

        InternalFlush();
        if (options_.auto_flush_interval_ > 0) {
            swn_flush_.Wait_For(std::chrono::seconds(options_.auto_flush_interval_));
        } else {
            swn_flush_.Wait();
        }
    }
}

void
SSDBImpl::StartMetricTask() {
    server::Metrics::GetInstance().KeepingAliveCounterIncrement(BACKGROUND_METRIC_INTERVAL);
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    fiu_do_on("DBImpl.StartMetricTask.InvalidTotalCache", cache_total = 0);

    if (cache_total > 0) {
        double cache_usage_double = cache_usage;
        server::Metrics::GetInstance().CpuCacheUsageGaugeSet(cache_usage_double * 100 / cache_total);
    } else {
        server::Metrics::GetInstance().CpuCacheUsageGaugeSet(0);
    }

    server::Metrics::GetInstance().GpuCacheUsageGaugeSet();
    /* SS TODO */
    // uint64_t size;
    // Size(size);
    // server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
    server::Metrics::GetInstance().CPUUsagePercentSet();
    server::Metrics::GetInstance().RAMUsagePercentSet();
    server::Metrics::GetInstance().GPUPercentGaugeSet();
    server::Metrics::GetInstance().GPUMemoryUsageGaugeSet();
    server::Metrics::GetInstance().OctetsSet();

    server::Metrics::GetInstance().CPUCoreUsagePercentSet();
    server::Metrics::GetInstance().GPUTemperature();
    server::Metrics::GetInstance().CPUTemperature();
    server::Metrics::GetInstance().PushToGateway();
}

void
SSDBImpl::BackgroundMetricThread() {
    SetThreadName("metric_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background metric thread exit";
            break;
        }

        swn_metric_.Wait_For(std::chrono::seconds(BACKGROUND_METRIC_INTERVAL));
        StartMetricTask();
        meta::FilesHolder::PrintInfo();
    }
}

void
SSDBImpl::StartBuildIndexTask() {
    // build index has been finished?
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (!index_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (index_thread_results_.back().wait_for(span) == std::future_status::ready) {
                index_thread_results_.pop_back();
            }
        }
    }

    // add new build index task
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (index_thread_results_.empty()) {
            index_thread_results_.push_back(index_thread_pool_.enqueue(&SSDBImpl::BackgroundWaitBuildIndex, this));
        }
    }
}

void
SSDBImpl::BackgroundWaitBuildIndex() {
    // TODO: update segment to index state and wait BackgroundIndexThread to build index
}

void
SSDBImpl::BackgroundIndexThread() {
    SetThreadName("index_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            WaitMergeFileFinish();
            WaitBuildIndexFinish();

            LOG_ENGINE_DEBUG_ << "DB background thread exit";
            break;
        }

        swn_index_.Wait_For(std::chrono::seconds(BACKGROUND_INDEX_INTERVAL));

        WaitMergeFileFinish();
        StartBuildIndexTask();
    }
}

void
SSDBImpl::BackgroundWalThread() {
    SetThreadName("wal_thread");
    server::SystemInfo::GetInstance().Init();

    std::chrono::system_clock::time_point next_auto_flush_time;
    auto get_next_auto_flush_time = [&]() {
        return std::chrono::system_clock::now() + std::chrono::seconds(options_.auto_flush_interval_);
    };
    if (options_.auto_flush_interval_ > 0) {
        next_auto_flush_time = get_next_auto_flush_time();
    }

    InternalFlush();
    while (true) {
        if (options_.auto_flush_interval_ > 0) {
            if (std::chrono::system_clock::now() >= next_auto_flush_time) {
                InternalFlush();
                next_auto_flush_time = get_next_auto_flush_time();
            }
        }

        wal::MXLogRecord record;
        auto error_code = wal_mgr_->GetNextRecord(record);
        if (error_code != WAL_SUCCESS) {
            LOG_ENGINE_ERROR_ << "WAL background GetNextRecord error";
            break;
        }

        if (record.type != wal::MXLogType::None) {
            ExecWalRecord(record);
            if (record.type == wal::MXLogType::Flush) {
                // notify flush request to return
                flush_req_swn_.Notify();

                // if user flush all manually, update auto flush also
                if (record.collection_id.empty() && options_.auto_flush_interval_ > 0) {
                    next_auto_flush_time = get_next_auto_flush_time();
                }
            }

        } else {
            if (!initialized_.load(std::memory_order_acquire)) {
                InternalFlush();
                flush_req_swn_.Notify();
                // SS TODO
                // WaitMergeFileFinish();
                // WaitBuildIndexFinish();
                LOG_ENGINE_DEBUG_ << "WAL background thread exit";
                break;
            }

            if (options_.auto_flush_interval_ > 0) {
                swn_wal_.Wait_Until(next_auto_flush_time);
            } else {
                swn_wal_.Wait();
            }
        }
    }
}

void
SSDBImpl::StartMergeTask(const std::set<std::string>& merge_collection_ids, bool force_merge_all) {
    // LOG_ENGINE_DEBUG_ << "Begin StartMergeTask";
    // merge task has been finished?
    {
        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        if (!merge_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (merge_thread_results_.back().wait_for(span) == std::future_status::ready) {
                merge_thread_results_.pop_back();
            }
        }
    }

    // add new merge task
    {
        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        if (merge_thread_results_.empty()) {
            // start merge file thread
            merge_thread_results_.push_back(
                merge_thread_pool_.enqueue(&SSDBImpl::BackgroundMerge, this, merge_collection_ids, force_merge_all));
        }
    }

    // LOG_ENGINE_DEBUG_ << "End StartMergeTask";
}

void
SSDBImpl::BackgroundMerge(std::set<std::string> collection_ids, bool force_merge_all) {
    // LOG_ENGINE_TRACE_ << " Background merge thread start";

    Status status;
    for (auto& collection_id : collection_ids) {
        // TODO: merge files
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection: " << collection_id;
            break;
        }
    }

    // TODO: cleanup with ttl
}

void
SSDBImpl::WaitMergeFileFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitMergeFileFinish";
    std::lock_guard<std::mutex> lck(merge_result_mutex_);
    for (auto& iter : merge_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitMergeFileFinish";
}

void
SSDBImpl::WaitBuildIndexFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitBuildIndexFinish";
    std::lock_guard<std::mutex> lck(index_result_mutex_);
    for (auto& iter : index_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitBuildIndexFinish";
}

Status
SSDBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
    //    auto collections_flushed = [&](const std::string collection_id,
    //                                   const std::set<std::string>& target_collection_names) -> uint64_t {
    //        uint64_t max_lsn = 0;
    //        if (options_.wal_enable_ && !target_collection_names.empty()) {
    //            uint64_t lsn = 0;
    //            for (auto& collection_name : target_collection_names) {
    //                snapshot::ScopedSnapshotT ss;
    //                snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    //                lsn = ss->GetMaxLsn();
    //                if (lsn > max_lsn) {
    //                    max_lsn = lsn;
    //                }
    //            }
    //            wal_mgr_->CollectionFlushed(collection_id, lsn);
    //        }
    //
    //        std::set<std::string> merge_collection_ids;
    //        for (auto& collection : target_collection_names) {
    //            merge_collection_ids.insert(collection);
    //        }
    //        StartMergeTask(merge_collection_ids);
    //        return max_lsn;
    //    };
    //
    //    auto force_flush_if_mem_full = [&]() -> uint64_t {
    //        if (mem_mgr_->GetCurrentMem() > options_.insert_buffer_size_) {
    //            LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] ", "insert", 0) << "Insert buffer size exceeds limit. Force
    //            flush"; InternalFlush();
    //        }
    //    };
    //
    //    Status status;
    //
    //    switch (record.type) {
    //        case wal::MXLogType::Entity: {
    //            snapshot::ScopedSnapshotT ss;
    //            status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
    //            if (!status.ok()) {
    //                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
    //                return status;
    //            }
    //            snapshot::PartitionPtr part = ss->GetPartition(record.partition_tag);
    //            if (part == nullptr) {
    //                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
    //                return status;
    //            }
    //
    //            status = mem_mgr_->InsertEntities(
    //                target_collection_name, record.length, record.ids, (record.data_size / record.length /
    //                sizeof(float)), (const float*)record.data, record.attr_nbytes, record.attr_data_size,
    //                record.attr_data, record.lsn);
    //            force_flush_if_mem_full();
    //
    //            // metrics
    //            milvus::server::CollectInsertMetrics metrics(record.length, status);
    //            break;
    //        }
    //        case wal::MXLogType::InsertBinary: {
    //            std::string target_collection_name;
    //            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
    //            if (!status.ok()) {
    //                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
    //                return status;
    //            }
    //
    //            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
    //                                             (record.data_size / record.length / sizeof(uint8_t)),
    //                                             (const u_int8_t*)record.data, record.lsn);
    //            force_flush_if_mem_full();
    //
    //            // metrics
    //            milvus::server::CollectInsertMetrics metrics(record.length, status);
    //            break;
    //        }
    //
    //        case wal::MXLogType::InsertVector: {
    //            std::string target_collection_name;
    //            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
    //            if (!status.ok()) {
    //                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
    //                return status;
    //            }
    //
    //            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
    //                                             (record.data_size / record.length / sizeof(float)),
    //                                             (const float*)record.data, record.lsn);
    //            force_flush_if_mem_full();
    //
    //            // metrics
    //            milvus::server::CollectInsertMetrics metrics(record.length, status);
    //            break;
    //        }
    //
    //        case wal::MXLogType::Delete: {
    //            std::vector<meta::CollectionSchema> partition_array;
    //            status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
    //            if (!status.ok()) {
    //                return status;
    //            }
    //
    //            std::vector<std::string> collection_ids{record.collection_id};
    //            for (auto& partition : partition_array) {
    //                auto& partition_collection_id = partition.collection_id_;
    //                collection_ids.emplace_back(partition_collection_id);
    //            }
    //
    //            if (record.length == 1) {
    //                for (auto& collection_id : collection_ids) {
    //                    status = mem_mgr_->DeleteVector(collection_id, *record.ids, record.lsn);
    //                    if (!status.ok()) {
    //                        return status;
    //                    }
    //                }
    //            } else {
    //                for (auto& collection_id : collection_ids) {
    //                    status = mem_mgr_->DeleteVectors(collection_id, record.length, record.ids, record.lsn);
    //                    if (!status.ok()) {
    //                        return status;
    //                    }
    //                }
    //            }
    //            break;
    //        }
    //
    //        case wal::MXLogType::Flush: {
    //            if (!record.collection_id.empty()) {
    //                // flush one collection
    //                std::vector<meta::CollectionSchema> partition_array;
    //                status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
    //                if (!status.ok()) {
    //                    return status;
    //                }
    //
    //                std::vector<std::string> collection_ids{record.collection_id};
    //                for (auto& partition : partition_array) {
    //                    auto& partition_collection_id = partition.collection_id_;
    //                    collection_ids.emplace_back(partition_collection_id);
    //                }
    //
    //                std::set<std::string> flushed_collections;
    //                for (auto& collection_id : collection_ids) {
    //                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
    //                    status = mem_mgr_->Flush(collection_id);
    //                    if (!status.ok()) {
    //                        break;
    //                    }
    //                    flushed_collections.insert(collection_id);
    //
    //                    status = FlushAttrsIndex(collection_id);
    //                    if (!status.ok()) {
    //                        return status;
    //                    }
    //                }
    //
    //                collections_flushed(record.collection_id, flushed_collections);
    //
    //            } else {
    //                // flush all collections
    //                std::set<std::string> collection_ids;
    //                {
    //                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
    //                    status = mem_mgr_->Flush(collection_ids);
    //                }
    //
    //                uint64_t lsn = collections_flushed("", collection_ids);
    //                if (options_.wal_enable_) {
    //                    wal_mgr_->RemoveOldFiles(lsn);
    //                }
    //            }
    //            break;
    //        }
    //
    //        default:
    //            break;
    //    }
    //
    //    return status;
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
