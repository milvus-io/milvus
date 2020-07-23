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
#include "config/ServerConfig.h"
#include "db/IDGenerator.h"
#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/merge/MergeManagerFactory.h"
#include "db/merge/SSMergeTask.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshots.h"
#include "insert/MemManagerFactory.h"
#include "knowhere/index/vector_index/helpers/BuilderSuspend.h"
#include "metrics/Metrics.h"
#include "metrics/SystemInfo.h"
#include "scheduler/Definition.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SSSearchJob.h"
#include "segment/SSSegmentReader.h"
#include "segment/SSSegmentWriter.h"
#include "utils/Exception.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
#include "wal/WalDefinations.h"

#include <fiu-local.h>
#include <src/scheduler/job/SSBuildIndexJob.h>
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
    merge_mgr_ptr_ = MergeManagerFactory::SSBuild(options_);

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

    // snapshot
    auto store = snapshot::Store::Build(config.storage.path(), config.general.meta_uri());
    snapshot::OperationExecutor::Init(store);
    snapshot::OperationExecutor::GetInstance().Start();
    snapshot::EventExecutor::Init(store);
    snapshot::EventExecutor::GetInstance().Start();
    snapshot::Snapshots::GetInstance().Init(store);

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    // TODO: merge files

    // wal
    if (options_.wal_enable_) {
        return Status(SERVER_NOT_IMPLEMENT, "Wal not implemented");
        //        auto error_code = DB_ERROR;
        //        if (wal_mgr_ != nullptr) {
        //            error_code = wal_mgr_->Init();
        //        }
        //        if (error_code != WAL_SUCCESS) {
        //            throw Exception(error_code, "Wal init error!");
        //        }
        //
        //        // recovery
        //        while (true) {
        //            wal::MXLogRecord record;
        //            auto error_code = wal_mgr_->GetNextRecovery(record);
        //            if (error_code != WAL_SUCCESS) {
        //                throw Exception(error_code, "Wal recovery error!");
        //            }
        //            if (record.type == wal::MXLogType::None) {
        //                break;
        //            }
        //            ExecWalRecord(record);
        //        }
        //
        //        // for distribute version, some nodes are read only
        //        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        //            // background wal thread
        //            bg_wal_thread_ = std::thread(&SSDBImpl::TimingWalThread, this);
        //        }
    } else {
        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background flush thread
            bg_flush_thread_ = std::thread(&SSDBImpl::TimingFlushThread, this);
        }
    }

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background build index thread
        bg_index_thread_ = std::thread(&SSDBImpl::TimingIndexThread, this);
    }

    // background metric thread
    fiu_do_on("options_metric_enable", options_.metric_enable_ = true);
    if (options_.metric_enable_) {
        bg_metric_thread_ = std::thread(&SSDBImpl::TimingMetricThread, this);
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
            //            // wait wal thread finish
            //            swn_wal_.Notify();
            //            bg_wal_thread_.join();
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

    snapshot::EventExecutor::GetInstance().Stop();
    snapshot::OperationExecutor::GetInstance().Stop();

    // LOG_ENGINE_TRACE_ << "DB service stop";
    return Status::OK();
}

Status
SSDBImpl::CreateCollection(const snapshot::CreateCollectionContext& context) {
    CHECK_INITIALIZED;

    auto ctx = context;
    // check uid existence/validation
    bool has_uid = false;
    for (auto& pair : ctx.fields_schema) {
        if (pair.first->GetFtype() == meta::hybrid::DataType::UID) {
            has_uid = true;
            break;
        }
    }

    // add uid field if not specified
    if (!has_uid) {
        auto uid_field = std::make_shared<snapshot::Field>(DEFAULT_UID_NAME, 0, milvus::engine::FieldType::UID);
        auto bloom_filter_element = std::make_shared<snapshot::FieldElement>(
            0, 0, DEFAULT_BLOOM_FILTER_NAME, milvus::engine::FieldElementType::FET_BLOOM_FILTER);
        auto delete_doc_element = std::make_shared<snapshot::FieldElement>(
            0, 0, DEFAULT_DELETED_DOCS_NAME, milvus::engine::FieldElementType::FET_DELETED_DOCS);

        ctx.fields_schema[uid_field] = {bloom_filter_element, delete_doc_element};
    }

    if (options_.wal_enable_) {
        //        ctx.lsn = wal_mgr_->CreateCollection(context.collection->GetName());
    }
    auto op = std::make_shared<snapshot::CreateCollectionOperation>(ctx);
    return op->Push();
}

Status
SSDBImpl::DescribeCollection(
    const std::string& collection_name, snapshot::CollectionPtr& collection,
    std::unordered_map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) {
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
SSDBImpl::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                         const std::vector<std::string>& field_names, bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<LoadVectorFieldHandler>(context, ss);
    handler->Iterate();

    return handler->GetStatus();
}

Status
SSDBImpl::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    snapshot::LSN_TYPE lsn = 0;
    if (options_.wal_enable_) {
        // SS TODO
        /* lsn = wal_mgr_->CreatePartition(collection_name, partition_tag); */
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
SSDBImpl::InsertEntities(const std::string& collection_name, const std::string& partition_name,
                         DataChunkPtr& data_chunk) {
    CHECK_INITIALIZED;

    if (data_chunk == nullptr) {
        return Status(DB_ERROR, "Null pointer");
    }

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto partition_ptr = ss->GetPartition(partition_name);
    if (partition_ptr == nullptr) {
        return Status(DB_NOT_FOUND, "Fail to get partition " + partition_name);
    }

    /* Generate id */
    if (data_chunk->fixed_fields_.find(engine::DEFAULT_UID_NAME) == data_chunk->fixed_fields_.end()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        IDNumbers ids;
        STATUS_CHECK(id_generator.GetNextIDNumbers(data_chunk->count_, ids));
        FIXED_FIELD_DATA& id_data = data_chunk->fixed_fields_[engine::DEFAULT_UID_NAME];
        id_data.resize(ids.size() * sizeof(int64_t));
        memcpy(id_data.data(), ids.data(), ids.size() * sizeof(int64_t));
    }

    if (options_.wal_enable_) {
        return Status(SERVER_NOT_IMPLEMENT, "Wal not implemented");
        //        auto vector_it = entity.vector_data_.begin();
        //        if (!vector_it->second.binary_data_.empty()) {
        //            wal_mgr_->InsertEntities(collection_name, partition_name, entity.id_array_,
        //            vector_it->second.binary_data_,
        //                                     attr_nbytes, attr_data);
        //        } else if (!vector_it->second.float_data_.empty()) {
        //            wal_mgr_->InsertEntities(collection_name, partition_name, entity.id_array_,
        //            vector_it->second.float_data_,
        //                                     attr_nbytes, attr_data);
        //        }
        //        swn_wal_.Notify();
    } else {
        // insert entities: collection_name is field id
        wal::MXLogRecord record;
        record.lsn = 0;
        record.collection_id = collection_name;
        record.partition_tag = partition_name;
        record.data_chunk = data_chunk;
        record.length = data_chunk->count_;
        record.type = wal::MXLogType::Entity;

        STATUS_CHECK(ExecWalRecord(record));
    }

    return Status::OK();
}

Status
SSDBImpl::DeleteEntities(const std::string& collection_name, engine::IDNumbers entity_ids) {
    CHECK_INITIALIZED;

    Status status;
    if (options_.wal_enable_) {
        return Status(SERVER_NOT_IMPLEMENT, "Wal not implemented");
        //        wal_mgr_->DeleteById(collection_name, entity_ids);
        //        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.type = wal::MXLogType::Delete;
        record.collection_id = collection_name;
        record.ids = entity_ids.data();
        record.length = entity_ids.size();

        status = ExecWalRecord(record);
    }

    return status;
}

Status
SSDBImpl::Flush(const std::string& collection_name) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    bool has_collection;
    status = HasCollection(collection_name, has_collection);
    if (!status.ok()) {
        return status;
    }
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection to flush does not exist: " << collection_name;
        return Status(DB_NOT_FOUND, "Collection to flush does not exist");
    }

    LOG_ENGINE_DEBUG_ << "Begin flush collection: " << collection_name;

    if (options_.wal_enable_) {
        return Status(SERVER_NOT_IMPLEMENT, "Wal not implemented");
        //        LOG_ENGINE_DEBUG_ << "WAL flush";
        //        auto lsn = wal_mgr_->Flush(collection_name);
        //        if (lsn != 0) {
        //            swn_wal_.Notify();
        //            flush_req_swn_.Wait();
        //        } else {
        //            // no collection flushed, call merge task to cleanup files
        //            std::set<std::string> merge_collection_names;
        //            StartMergeTask(merge_collection_names);
        //        }
    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush(collection_name);
    }

    LOG_ENGINE_DEBUG_ << "End flush collection: " << collection_name;

    return status;
}

Status
SSDBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Begin flush all collections";

    Status status;
    fiu_do_on("options_wal_enable_false", options_.wal_enable_ = false);
    if (options_.wal_enable_) {
        return Status(SERVER_NOT_IMPLEMENT, "Wal not implemented");
        //        LOG_ENGINE_DEBUG_ << "WAL flush";
        //        auto lsn = wal_mgr_->Flush();
        //        if (lsn != 0) {
        //            swn_wal_.Notify();
        //            flush_req_swn_.Wait();
        //        } else {
        //            // no collection flushed, call merge task to cleanup files
        //            std::set<std::string> merge_collection_names;
        //            StartMergeTask(merge_collection_names);
        //        }
    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush();
    }

    LOG_ENGINE_DEBUG_ << "End flush all collections";

    return status;
}

Status
SSDBImpl::Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                  double threshold) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Before compacting, wait for build index thread to finish...";
    const std::lock_guard<std::mutex> index_lock(build_index_mutex_);
    const std::lock_guard<std::mutex> merge_lock(flush_merge_compact_mutex_);

    Status status;
    bool has_collection;
    status = HasCollection(collection_name, has_collection);
    if (!status.ok()) {
        return status;
    }
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection to compact does not exist: " << collection_name;
        return Status(DB_NOT_FOUND, "Collection to compact does not exist");
    }

    snapshot::ScopedSnapshotT latest_ss;
    status = snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    if (!status.ok()) {
        return status;
    }

    auto& segments = latest_ss->GetResources<snapshot::Segment>();
    for (auto& kv : segments) {
        // client break the connection, no need to continue
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, stop compact operation";
            break;
        }

        snapshot::ID_TYPE segment_id = kv.first;
        auto read_visitor = engine::SegmentVisitor::Build(latest_ss, segment_id);
        segment::SSSegmentReaderPtr segment_reader =
            std::make_shared<segment::SSSegmentReader>(options_.meta_.path_, read_visitor);

        segment::DeletedDocsPtr deleted_docs;
        status = segment_reader->LoadDeletedDocs(deleted_docs);
        if (!status.ok() || deleted_docs == nullptr) {
            continue;  // no deleted docs, no need to compact
        }

        auto segment_commit = latest_ss->GetSegmentCommitBySegmentId(segment_id);
        auto row_count = segment_commit->GetRowCount();
        if (row_count == 0) {
            continue;
        }

        auto deleted_count = deleted_docs->GetSize();
        if (deleted_count / (row_count + deleted_count) < threshold) {
            continue;  // no need to compact
        }

        snapshot::IDS_TYPE ids = {segment_id};
        SSMergeTask merge_task(options_, latest_ss, ids);
        status = merge_task.Execute();
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Compact failed for segment " << segment_reader->GetSegmentPath() << ": "
                              << status.message();
            continue;  // skip this file and try compact next one
        }
    }

    return status;
}

Status
SSDBImpl::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                        const std::vector<std::string>& field_names, DataChunkPtr& data_chunk) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    std::string dir_root = options_.meta_.path_;
    auto handler = std::make_shared<GetEntityByIdSegmentHandler>(nullptr, ss, dir_root, id_array, field_names);
    handler->Iterate();
    STATUS_CHECK(handler->GetStatus());

    data_chunk = handler->data_chunk_;
    return Status::OK();
}

Status
SSDBImpl::GetEntityIDs(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto read_visitor = engine::SegmentVisitor::Build(ss, segment_id);
    segment::SSSegmentReaderPtr segment_reader =
        std::make_shared<segment::SSSegmentReader>(options_.meta_.path_, read_visitor);

    STATUS_CHECK(segment_reader->LoadUids(entity_ids));

    return Status::OK();
}

Status
SSDBImpl::CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                      const std::string& field_name, const CollectionIndex& index) {
    CHECK_INITIALIZED;

    // step 1: wait merge file thread finished to avoid duplicate data bug
    auto status = Flush();
    WaitMergeFileFinish();  // let merge file thread finish

    // step 2: compare old index and new index
    CollectionIndex new_index = index;
    CollectionIndex old_index;
    status = DescribeIndex(collection_name, field_name, old_index);
    if (!status.ok()) {
        return status;
    }

    if (old_index.metric_type_ != (int32_t)MetricType::INVALID) {
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateCollection
    }
    if (utils::IsSameIndex(old_index, new_index)) {
        return Status::OK();  // same index
    }

    // step 3: drop old index
    DropIndex(collection_name);
    WaitMergeFileFinish();  // let merge file thread finish since DropIndex start a merge task

    // step 4: create field element for index
    status = SetSnapshotIndex(collection_name, field_name, new_index);
    if (!status.ok()) {
        return status;
    }

    // step 5: start background build index thread
    std::vector<std::string> collection_names = {collection_name};
    WaitBuildIndexFinish();
    StartBuildIndexTask(collection_names);

    // step 6: iterate segments need to be build index, wait until all segments are built
    while (true) {
        SnapshotVisitor ss_visitor(collection_name);
        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex(field_name, segment_ids);
        if (segment_ids.empty()) {
            break;
        }

        index_req_swn_.Wait_For(std::chrono::seconds(1));

        // client break the connection, no need to block, check every 1 second
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, build index in background";
            break;  // just break, not return, continue to update partitions files to to_index
        }
    }

    return Status::OK();
}

Status
SSDBImpl::DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) {
    CHECK_INITIALIZED;

    return GetSnapshotIndex(collection_name, field_name, index);
}

Status
SSDBImpl::DropIndex(const std::string& collection_name, const std::string& field_name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_name;

    STATUS_CHECK(DeleteSnapshotIndex(collection_name, field_name));

    std::set<std::string> merge_collection_names = {collection_name};
    StartMergeTask(merge_collection_names, true);
    return Status::OK();
}

Status
SSDBImpl::DropIndex(const std::string& collection_name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_name;

    std::vector<std::string> field_names;
    {
        snapshot::ScopedSnapshotT ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
        field_names = ss->GetFieldNames();
    }

    snapshot::OperationContext context;
    for (auto& field_name : field_names) {
        STATUS_CHECK(DeleteSnapshotIndex(collection_name, field_name));
    }

    std::set<std::string> merge_collection_names = {collection_name};
    StartMergeTask(merge_collection_names, true);
    return Status::OK();
}

Status
SSDBImpl::Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) {
    CHECK_INITIALIZED;

    TimeRecorder rc("SSDBImpl::Query");

    scheduler::SSSearchJobPtr job = std::make_shared<scheduler::SSSearchJob>(nullptr, options_, query_ptr);

    /* put search job to scheduler and wait job finish */
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitFinish();

    if (!job->status().ok()) {
        return job->status();
    }

    //    snapshot::ScopedSnapshotT ss;
    //    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
    //
    //    /* collect all valid segment */
    //    std::vector<SegmentVisitor::Ptr> segment_visitors;
    //    auto exec = [&] (const snapshot::Segment::Ptr& segment, snapshot::SegmentIterator* handler) -> Status {
    //        auto p_id = segment->GetPartitionId();
    //        auto p_ptr = ss->GetResource<snapshot::Partition>(p_id);
    //        auto& p_name = p_ptr->GetName();
    //
    //        /* check partition match pattern */
    //        bool match = false;
    //        if (partition_patterns.empty()) {
    //            match = true;
    //        } else {
    //            for (auto &pattern : partition_patterns) {
    //                if (StringHelpFunctions::IsRegexMatch(p_name, pattern)) {
    //                    match = true;
    //                    break;
    //                }
    //            }
    //        }
    //
    //        if (match) {
    //            auto visitor = SegmentVisitor::Build(ss, segment->GetID());
    //            if (!visitor) {
    //                return Status(milvus::SS_ERROR, "Cannot build segment visitor");
    //            }
    //            segment_visitors.push_back(visitor);
    //        }
    //        return Status::OK();
    //    };
    //
    //    auto segment_iter = std::make_shared<snapshot::SegmentIterator>(ss, exec);
    //    segment_iter->Iterate();
    //    STATUS_CHECK(segment_iter->GetStatus());
    //
    //    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, segment count: %ld", segment_visitors.size());
    //
    //    VectorsData vectors;
    //    scheduler::SSSearchJobPtr job =
    //        std::make_shared<scheduler::SSSearchJob>(tracer.Context(), general_query, query_ptr, attr_type, vectors);
    //    for (auto& sv : segment_visitors) {
    //        job->AddSegmentVisitor(sv);
    //    }
    //
    //    // step 2: put search job to scheduler and wait result
    //    scheduler::JobMgrInst::GetInstance()->Put(job);
    //    job->WaitResult();
    //
    //    if (!job->GetStatus().ok()) {
    //        return job->GetStatus();
    //    }
    //
    //    // step 3: construct results
    //    result.row_num_ = job->vector_count();
    //    result.result_ids_ = job->GetResultIds();
    //    result.result_distances_ = job->GetResultDistances();

    // step 4: get entities by result ids
    // STATUS_CHECK(GetEntityByID(collection_name, result.result_ids_, field_names, result.vectors_, result.attrs_));

    // step 5: filter entities by field names
    //    std::vector<engine::AttrsData> filter_attrs;
    //    for (auto attr : result.attrs_) {
    //        AttrsData attrs_data;
    //        attrs_data.attr_type_ = attr.attr_type_;
    //        attrs_data.attr_count_ = attr.attr_count_;
    //        attrs_data.id_array_ = attr.id_array_;
    //        for (auto& name : field_names) {
    //            if (attr.attr_data_.find(name) != attr.attr_data_.end()) {
    //                attrs_data.attr_data_.insert(std::make_pair(name, attr.attr_data_.at(name)));
    //            }
    //        }
    //        filter_attrs.emplace_back(attrs_data);
    //    }

    rc.ElapseFromBegin("Engine query totally cost");

    // tracer.Context()->GetTraceContext()->GetSpan()->Finish();

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// Internal APIs
////////////////////////////////////////////////////////////////////////////////
void
SSDBImpl::InternalFlush(const std::string& collection_name) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_name;
    ExecWalRecord(record);
}

void
SSDBImpl::TimingFlushThread() {
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
SSDBImpl::TimingMetricThread() {
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
SSDBImpl::StartBuildIndexTask(const std::vector<std::string>& collection_names) {
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
            index_thread_results_.push_back(
                index_thread_pool_.enqueue(&SSDBImpl::BackgroundBuildIndexTask, this, collection_names));
        }
    }
}

void
SSDBImpl::BackgroundBuildIndexTask(std::vector<std::string> collection_names) {
    std::unique_lock<std::mutex> lock(build_index_mutex_);

    for (auto collection_name : collection_names) {
        SnapshotVisitor ss_visitor(collection_name);

        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex("", segment_ids);

        scheduler::SSBuildIndexJobPtr job =
            std::make_shared<scheduler::SSBuildIndexJob>(options_, collection_name, segment_ids);

        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitFinish();

        if (!job->status().ok()) {
            LOG_ENGINE_ERROR_ << job->status().message();
            break;
        }
    }
}

void
SSDBImpl::TimingIndexThread() {
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

        std::vector<std::string> collection_names;
        snapshot::Snapshots::GetInstance().GetCollectionNames(collection_names);
        WaitMergeFileFinish();
        StartBuildIndexTask(collection_names);
    }
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

void
SSDBImpl::TimingWalThread() {
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

Status
SSDBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
    auto collections_flushed = [&](const std::string& collection_name,
                                   const std::set<std::string>& target_collection_names) -> uint64_t {
        uint64_t max_lsn = 0;
        if (options_.wal_enable_ && !target_collection_names.empty()) {
            //            uint64_t lsn = 0;
            //            for (auto& collection_name : target_collection_names) {
            //                snapshot::ScopedSnapshotT ss;
            //                snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
            //                lsn = ss->GetMaxLsn();
            //                if (lsn > max_lsn) {
            //                    max_lsn = lsn;
            //                }
            //            }
            //            wal_mgr_->CollectionFlushed(collection_name, lsn);
        }

        std::set<std::string> merge_collection_names;
        for (auto& collection : target_collection_names) {
            merge_collection_names.insert(collection);
        }
        StartMergeTask(merge_collection_names);
        return max_lsn;
    };

    auto force_flush_if_mem_full = [&]() -> void {
        if (mem_mgr_->GetCurrentMem() > options_.insert_buffer_size_) {
            LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] ", "insert", 0) << "Insert buffer size exceeds limit. Force flush";
            InternalFlush();
        }
    };

    auto get_collection_partition_id = [&](const wal::MXLogRecord& record, int64_t& col_id,
                                           int64_t& part_id) -> Status {
        snapshot::ScopedSnapshotT ss;
        auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get snapshot fail: " << status.message();
            return status;
        }
        col_id = ss->GetCollectionId();
        snapshot::PartitionPtr part = ss->GetPartition(record.partition_tag);
        if (part == nullptr) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
            return status;
        }
        part_id = part->GetID();

        return Status::OK();
    };

    Status status;

    switch (record.type) {
        case wal::MXLogType::Entity: {
            int64_t collection_name = 0, partition_id = 0;
            auto status = get_collection_partition_id(record, collection_name, partition_id);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << status.message();
                return status;
            }

            status = mem_mgr_->InsertEntities(collection_name, partition_id, record.data_chunk, record.lsn);
            force_flush_if_mem_full();

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::Delete: {
            snapshot::ScopedSnapshotT ss;
            auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "delete", 0) << "Get snapshot fail: " << status.message();
                return status;
            }

            if (record.length == 1) {
                status = mem_mgr_->DeleteEntity(ss->GetCollectionId(), *record.ids, record.lsn);
                if (!status.ok()) {
                    return status;
                }
            } else {
                status = mem_mgr_->DeleteEntities(ss->GetCollectionId(), record.length, record.ids, record.lsn);
                if (!status.ok()) {
                    return status;
                }
            }
            break;
        }

        case wal::MXLogType::Flush: {
            if (!record.collection_id.empty()) {
                // flush one collection
                snapshot::ScopedSnapshotT ss;
                auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
                if (!status.ok()) {
                    LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "flush", 0) << "Get snapshot fail: " << status.message();
                    return status;
                }

                const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                int64_t collection_name = ss->GetCollectionId();
                status = mem_mgr_->Flush(collection_name);
                if (!status.ok()) {
                    return status;
                }

                std::set<std::string> flushed_collections;
                collections_flushed(record.collection_id, flushed_collections);

            } else {
                // flush all collections
                std::set<int64_t> collection_names;
                {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(collection_names);
                }

                std::set<std::string> flushed_collections;
                for (auto id : collection_names) {
                    snapshot::ScopedSnapshotT ss;
                    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, id);
                    if (!status.ok()) {
                        LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "flush", 0) << "Get snapshot fail: " << status.message();
                        return status;
                    }

                    flushed_collections.insert(ss->GetName());
                }

                uint64_t lsn = collections_flushed("", flushed_collections);
                if (options_.wal_enable_) {
                    //                    wal_mgr_->RemoveOldFiles(lsn);
                }
            }
            break;
        }

        default:
            break;
    }

    return status;
}

void
SSDBImpl::StartMergeTask(const std::set<std::string>& collection_names, bool force_merge_all) {
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
                merge_thread_pool_.enqueue(&SSDBImpl::BackgroundMerge, this, collection_names, force_merge_all));
        }
    }

    // LOG_ENGINE_DEBUG_ << "End StartMergeTask";
}

void
SSDBImpl::BackgroundMerge(std::set<std::string> collection_names, bool force_merge_all) {
    // LOG_ENGINE_TRACE_ << " Background merge thread start";

    Status status;
    for (auto& collection_name : collection_names) {
        const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

        auto old_strategy = merge_mgr_ptr_->Strategy();
        if (force_merge_all) {
            merge_mgr_ptr_->UseStrategy(MergeStrategyType::ADAPTIVE);
        }

        auto status = merge_mgr_ptr_->MergeFiles(collection_name);
        merge_mgr_ptr_->UseStrategy(old_strategy);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_name
                              << " reason:" << status.message();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection: " << collection_name;
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
SSDBImpl::SuspendIfFirst() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (++live_search_num_ == 1) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuilderSuspend();
    }
}

void
SSDBImpl::ResumeIfLast() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (--live_search_num_ == 0) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuildResume();
    }
}

}  // namespace engine
}  // namespace milvus
