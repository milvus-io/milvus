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

#include "db/DBImpl.h"
#include "cache/CpuCacheMgr.h"
#include "codecs/Codec.h"
#include "config/ServerConfig.h"
#include "db/IDGenerator.h"
#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/merge/MergeManagerFactory.h"
#include "db/merge/MergeTask.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshots.h"
#include "insert/MemManagerFactory.h"
#include "knowhere/index/vector_index/helpers/BuilderSuspend.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"
#include "metrics/Metrics.h"
#include "metrics/SystemInfo.h"
#include "scheduler/Definition.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SearchJob.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Exception.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
#include "wal/WalDefinations.h"

#include <fiu-local.h>
#include <src/scheduler/job/BuildIndexJob.h>
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

DBImpl::DBImpl(const DBOptions& options)
    : options_(options), initialized_(false), merge_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    mem_mgr_ = MemManagerFactory::Build(options_);
    merge_mgr_ptr_ = MergeManagerFactory::SSBuild(options_);

    /* watch on storage.auto_flush_interval */
    ConfigMgr::GetInstance().Attach("storage.auto_flush_interval", this);

    Start();
}

DBImpl::~DBImpl() {
    ConfigMgr::GetInstance().Detach("storage.auto_flush_interval", this);

    Stop();
}

////////////////////////////////////////////////////////////////////////////////
// External APIs
////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // snapshot
    auto store = snapshot::Store::Build(options_.meta_.backend_uri_, options_.meta_.path_,
                                        codec::Codec::instance().GetSuffixSet());
    snapshot::OperationExecutor::Init(store);
    snapshot::OperationExecutor::GetInstance().Start();
    snapshot::EventExecutor::Init(store);
    snapshot::EventExecutor::GetInstance().Start();
    snapshot::Snapshots::GetInstance().Init(store);

    knowhere::enable_faiss_logging();

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    // TODO: merge files

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background flush thread
        bg_flush_thread_ = std::thread(&DBImpl::TimingFlushThread, this);
    }

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background build index thread
        bg_index_thread_ = std::thread(&DBImpl::TimingIndexThread, this);
    }

    // background metric thread
    fiu_do_on("options_metric_enable", options_.metric_enable_ = true);
    if (options_.metric_enable_) {
        bg_metric_thread_ = std::thread(&DBImpl::TimingMetricThread, this);
    }

    return Status::OK();
}

Status
DBImpl::Stop() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    initialized_.store(false, std::memory_order_release);

    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // flush all without merge
        wal::MXLogRecord record;
        record.type = wal::MXLogType::Flush;
        ExecWalRecord(record);

        // wait flush thread finish
        swn_flush_.Notify();
        bg_flush_thread_.join();

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
DBImpl::CreateCollection(const snapshot::CreateCollectionContext& context) {
    CHECK_INITIALIZED;

    auto ctx = context;

    // default id is auto-generated
    auto params = ctx.collection->GetParams();
    if (params.find(PARAM_UID_AUTOGEN) == params.end()) {
        params[PARAM_UID_AUTOGEN] = true;
        ctx.collection->SetParams(params);
    }

    // check uid existence
    snapshot::FieldPtr uid_field;
    for (auto& pair : ctx.fields_schema) {
        if (pair.first->GetName() == DEFAULT_UID_NAME) {
            uid_field = pair.first;
            break;
        }
    }

    // add uid field if not specified
    if (uid_field == nullptr) {
        uid_field = std::make_shared<snapshot::Field>(DEFAULT_UID_NAME, 0, DataType::INT64);
    }

    // define uid elements
    auto bloom_filter_element = std::make_shared<snapshot::FieldElement>(
        0, 0, DEFAULT_BLOOM_FILTER_NAME, milvus::engine::FieldElementType::FET_BLOOM_FILTER);
    auto delete_doc_element = std::make_shared<snapshot::FieldElement>(
        0, 0, DEFAULT_DELETED_DOCS_NAME, milvus::engine::FieldElementType::FET_DELETED_DOCS);
    ctx.fields_schema[uid_field] = {bloom_filter_element, delete_doc_element};

    auto op = std::make_shared<snapshot::CreateCollectionOperation>(ctx);
    return op->Push();
}

Status
DBImpl::DropCollection(const std::string& name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Prepare to drop collection " << name;

    snapshot::ScopedSnapshotT ss;
    auto& snapshots = snapshot::Snapshots::GetInstance();
    STATUS_CHECK(snapshots.GetSnapshot(ss, name));

    mem_mgr_->EraseMem(ss->GetCollectionId());  // not allow insert

    return snapshots.DropCollection(ss->GetCollectionId(), std::numeric_limits<snapshot::LSN_TYPE>::max());
}

Status
DBImpl::HasCollection(const std::string& collection_name, bool& has_or_not) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    has_or_not = status.ok();

    return Status::OK();
}

Status
DBImpl::ListCollections(std::vector<std::string>& names) {
    CHECK_INITIALIZED;

    names.clear();
    return snapshot::Snapshots::GetInstance().GetCollectionNames(names);
}

Status
DBImpl::GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                          snapshot::FieldElementMappings& fields_schema) {
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
DBImpl::GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) {
    CHECK_INITIALIZED;

    STATUS_CHECK(GetSnapshotInfo(collection_name, collection_stats));
    return Status::OK();
}

Status
DBImpl::CountEntities(const std::string& collection_name, int64_t& row_count) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    row_count = ss->GetCollectionCommit()->GetRowCount();
    return Status::OK();
}

Status
DBImpl::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    snapshot::LSN_TYPE lsn = 0;
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
DBImpl::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    // SS TODO: Is below step needed? Or How to implement it?
    /* mem_mgr_->EraseMem(partition_name); */

    snapshot::PartitionContext context;
    context.name = partition_name;
    auto op = std::make_shared<snapshot::DropPartitionOperation>(context, ss);
    return op->Push();
}

Status
DBImpl::HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto partition_tags = std::move(ss->GetPartitionNames());
    for (auto& tag : partition_tags) {
        if (tag == partition_tag) {
            exist = true;
            return Status::OK();
        }
    }

    exist = false;
    return Status::OK();
}

Status
DBImpl::ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    partition_names = std::move(ss->GetPartitionNames());
    return Status::OK();
}

Status
DBImpl::CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                    const std::string& field_name, const CollectionIndex& index) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Create index for collection: " << collection_name << " field: " << field_name;

    // step 1: wait merge file thread finished to avoid duplicate data bug
    auto status = Flush();
    WaitMergeFileFinish();  // let merge file thread finish

    // step 2: compare old index and new index
    CollectionIndex new_index = index;
    CollectionIndex old_index;
    STATUS_CHECK(GetSnapshotIndex(collection_name, field_name, old_index));

    if (utils::IsSameIndex(old_index, new_index)) {
        return Status::OK();  // same index
    }

    // step 3: drop old index
    DropIndex(collection_name, field_name);
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
DBImpl::DropIndex(const std::string& collection_name, const std::string& field_name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_name << " field: " << field_name;

    STATUS_CHECK(DeleteSnapshotIndex(collection_name, field_name));

    std::set<std::string> merge_collection_names = {collection_name};
    StartMergeTask(merge_collection_names, true);

    return Status::OK();
}

Status
DBImpl::DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Describe index for collection: " << collection_name << " field: " << field_name;

    STATUS_CHECK(GetSnapshotIndex(collection_name, field_name, index));

    return Status::OK();
}

Status
DBImpl::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk) {
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

    auto id_field = ss->GetField(DEFAULT_UID_NAME);
    if (id_field == nullptr) {
        return Status(DB_ERROR, "Field '_id' not found");
    }

    auto& params = ss->GetCollection()->GetParams();
    bool auto_increment = true;
    if (params.find(PARAM_UID_AUTOGEN) != params.end()) {
        auto_increment = params[PARAM_UID_AUTOGEN];
    }

    FIXEDX_FIELD_MAP& fields = data_chunk->fixed_fields_;
    auto pair = fields.find(engine::DEFAULT_UID_NAME);
    if (auto_increment) {
        // id is auto increment, but client provides id, return error
        if (pair != fields.end() && pair->second != nullptr) {
            return Status(DB_ERROR, "Field '_id' is auto increment, no need to provide id");
        }
    } else {
        // id is not auto increment, but client doesn't provide id, return error
        if (pair == fields.end() || pair->second == nullptr) {
            return Status(DB_ERROR, "Field '_id' is user defined");
        }
    }

    // generate id
    if (auto_increment) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        IDNumbers ids;
        STATUS_CHECK(id_generator.GetNextIDNumbers(data_chunk->count_, ids));
        BinaryDataPtr id_data = std::make_shared<BinaryData>();
        id_data->data_.resize(ids.size() * sizeof(int64_t));
        memcpy(id_data->data_.data(), ids.data(), ids.size() * sizeof(int64_t));
        data_chunk->fixed_fields_[engine::DEFAULT_UID_NAME] = id_data;
    }

    // insert entities: collection_name is field id
    wal::MXLogRecord record;
    record.lsn = 0;
    record.collection_id = collection_name;
    record.partition_tag = partition_name;
    record.data_chunk = data_chunk;
    record.length = data_chunk->count_;
    record.type = wal::MXLogType::Entity;

    STATUS_CHECK(ExecWalRecord(record));

    return Status::OK();
}

Status
DBImpl::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                      const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                      DataChunkPtr& data_chunk) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    std::string dir_root = options_.meta_.path_;
    valid_row.resize(id_array.size(), false);
    auto handler =
        std::make_shared<GetEntityByIdSegmentHandler>(nullptr, ss, dir_root, id_array, field_names, valid_row);
    handler->Iterate();
    STATUS_CHECK(handler->GetStatus());

    data_chunk = handler->data_chunk_;
    return Status::OK();
}

Status
DBImpl::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids) {
    CHECK_INITIALIZED;

    Status status;
    wal::MXLogRecord record;
    record.lsn = 0;  // need to get from meta ?
    record.type = wal::MXLogType::Delete;
    record.collection_id = collection_name;
    record.ids = entity_ids.data();
    record.length = entity_ids.size();

    status = ExecWalRecord(record);

    return status;
}

Status
DBImpl::Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) {
    CHECK_INITIALIZED;

    TimeRecorder rc("DBImpl::Query");

    if (!query_ptr->root) {
        return Status{DB_ERROR, "BinaryQuery is null"};
    }

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, query_ptr->collection_id));

    /* collect all valid segment */
    std::vector<SegmentVisitor::Ptr> segment_visitors;
    auto exec = [&](const snapshot::Segment::Ptr& segment, snapshot::SegmentIterator* handler) -> Status {
        auto p_id = segment->GetPartitionId();
        auto p_ptr = ss->GetResource<snapshot::Partition>(p_id);
        auto& p_name = p_ptr->GetName();

        /* check partition match pattern */
        bool match = false;
        if (query_ptr->partitions.empty()) {
            match = true;
        } else {
            for (auto& pattern : query_ptr->partitions) {
                if (StringHelpFunctions::IsRegexMatch(p_name, pattern)) {
                    match = true;
                    break;
                }
            }
        }

        if (match) {
            auto visitor = SegmentVisitor::Build(ss, segment->GetID());
            if (!visitor) {
                return Status(milvus::SS_ERROR, "Cannot build segment visitor");
            }
            segment_visitors.push_back(visitor);
        }
        return Status::OK();
    };

    auto segment_iter = std::make_shared<snapshot::SegmentIterator>(ss, exec);
    segment_iter->Iterate();
    STATUS_CHECK(segment_iter->GetStatus());

    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, segment count: %ld", segment_visitors.size());

    engine::snapshot::IDS_TYPE segment_ids;
    for (auto& sv : segment_visitors) {
        segment_ids.emplace_back(sv->GetSegment()->GetID());
    }

    scheduler::SearchJobPtr job = std::make_shared<scheduler::SearchJob>(nullptr, ss, options_, query_ptr, segment_ids);

    cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info before query
    /* put search job to scheduler and wait job finish */
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitFinish();
    cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info after query

    if (!job->status().ok()) {
        return job->status();
    }

    if (job->query_result()) {
        result = job->query_result();
    }

    // step 4: get entities by result ids
    std::vector<bool> valid_row;
    if (!query_ptr->field_names.empty()) {
        STATUS_CHECK(GetEntityByID(query_ptr->collection_id, result->result_ids_, query_ptr->field_names, valid_row,
                                   result->data_chunk_));
    }

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

Status
DBImpl::ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto read_visitor = engine::SegmentVisitor::Build(ss, segment_id);
    if (!read_visitor) {
        return Status(SERVER_FILE_NOT_FOUND, "Segment not exist");
    }
    segment::SegmentReaderPtr segment_reader =
        std::make_shared<segment::SegmentReader>(options_.meta_.path_, read_visitor);

    STATUS_CHECK(segment_reader->LoadUids(entity_ids));

    return Status::OK();
}

Status
DBImpl::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                       const std::vector<std::string>& field_names, bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<LoadVectorFieldHandler>(context, ss);
    handler->Iterate();

    return handler->GetStatus();
}

Status
DBImpl::Flush(const std::string& collection_name) {
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
    InternalFlush(collection_name);
    LOG_ENGINE_DEBUG_ << "End flush collection: " << collection_name;

    return status;
}

Status
DBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Begin flush all collections";
    InternalFlush();
    LOG_ENGINE_DEBUG_ << "End flush all collections";

    return Status::OK();
}

Status
DBImpl::Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_name, double threshold) {
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
        segment::SegmentReaderPtr segment_reader =
            std::make_shared<segment::SegmentReader>(options_.meta_.path_, read_visitor);

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

        auto deleted_count = deleted_docs->GetCount();
        if (deleted_count / (row_count + deleted_count) < threshold) {
            continue;  // no need to compact
        }

        snapshot::IDS_TYPE ids = {segment_id};
        MergeTask merge_task(options_, latest_ss, ids);
        status = merge_task.Execute();
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Compact failed for segment " << segment_reader->GetSegmentPath() << ": "
                              << status.message();
            continue;  // skip this file and try compact next one
        }
    }

    return status;
}

////////////////////////////////////////////////////////////////////////////////
// Internal APIs
////////////////////////////////////////////////////////////////////////////////
void
DBImpl::InternalFlush(const std::string& collection_name) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_name;
    ExecWalRecord(record);
}

void
DBImpl::TimingFlushThread() {
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
DBImpl::StartMetricTask() {
    server::Metrics::GetInstance().KeepingAliveCounterIncrement(BACKGROUND_METRIC_INTERVAL);
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance().CacheUsage();
    int64_t cache_total = cache::CpuCacheMgr::GetInstance().CacheCapacity();
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
DBImpl::TimingMetricThread() {
    SetThreadName("metric_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background metric thread exit";
            break;
        }

        swn_metric_.Wait_For(std::chrono::seconds(BACKGROUND_METRIC_INTERVAL));
        StartMetricTask();
    }
}

void
DBImpl::StartBuildIndexTask(const std::vector<std::string>& collection_names) {
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
                index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndexTask, this, collection_names));
        }
    }
}

void
DBImpl::BackgroundBuildIndexTask(std::vector<std::string> collection_names) {
    std::unique_lock<std::mutex> lock(build_index_mutex_);

    for (auto collection_name : collection_names) {
        snapshot::ScopedSnapshotT latest_ss;
        auto status = snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        if (!status.ok()) {
            return;
        }
        SnapshotVisitor ss_visitor(latest_ss);

        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex("", segment_ids);
        if (segment_ids.empty()) {
            continue;
        }

        LOG_ENGINE_DEBUG_ << "Create BuildIndexJob for " << segment_ids.size() << " segments of " << collection_name;
        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info before build index
        scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(latest_ss, options_, segment_ids);
        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitFinish();
        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info after build index

        if (!job->status().ok()) {
            LOG_ENGINE_ERROR_ << job->status().message();
            break;
        }
    }
}

void
DBImpl::TimingIndexThread() {
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
DBImpl::WaitBuildIndexFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitBuildIndexFinish";
    std::lock_guard<std::mutex> lck(index_result_mutex_);
    for (auto& iter : index_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitBuildIndexFinish";
}

void
DBImpl::TimingWalThread() {
    //    SetThreadName("wal_thread");
    //    server::SystemInfo::GetInstance().Init();
    //
    //    std::chrono::system_clock::time_point next_auto_flush_time;
    //    auto get_next_auto_flush_time = [&]() {
    //        return std::chrono::system_clock::now() + std::chrono::seconds(options_.auto_flush_interval_);
    //    };
    //    if (options_.auto_flush_interval_ > 0) {
    //        next_auto_flush_time = get_next_auto_flush_time();
    //    }
    //
    //    InternalFlush();
    //    while (true) {
    //        if (options_.auto_flush_interval_ > 0) {
    //            if (std::chrono::system_clock::now() >= next_auto_flush_time) {
    //                InternalFlush();
    //                next_auto_flush_time = get_next_auto_flush_time();
    //            }
    //        }
    //
    //        wal::MXLogRecord record;
    //        auto error_code = wal_mgr_->GetNextRecord(record);
    //        if (error_code != WAL_SUCCESS) {
    //            LOG_ENGINE_ERROR_ << "WAL background GetNextRecord error";
    //            break;
    //        }
    //
    //        if (record.type != wal::MXLogType::None) {
    //            ExecWalRecord(record);
    //            if (record.type == wal::MXLogType::Flush) {
    //                // notify flush request to return
    //                flush_req_swn_.Notify();
    //
    //                // if user flush all manually, update auto flush also
    //                if (record.collection_id.empty() && options_.auto_flush_interval_ > 0) {
    //                    next_auto_flush_time = get_next_auto_flush_time();
    //                }
    //            }
    //
    //        } else {
    //            if (!initialized_.load(std::memory_order_acquire)) {
    //                InternalFlush();
    //                flush_req_swn_.Notify();
    //                // SS TODO
    //                // WaitMergeFileFinish();
    //                // WaitBuildIndexFinish();
    //                LOG_ENGINE_DEBUG_ << "WAL background thread exit";
    //                break;
    //            }
    //
    //            if (options_.auto_flush_interval_ > 0) {
    //                swn_wal_.Wait_Until(next_auto_flush_time);
    //            } else {
    //                swn_wal_.Wait();
    //            }
    //        }
    //    }
}

Status
DBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
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
            status = get_collection_partition_id(record, collection_name, partition_id);
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
            status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
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
                status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, record.collection_id);
                if (!status.ok()) {
                    LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "flush", 0) << "Get snapshot fail: " << status.message();
                    return status;
                }

                const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                int64_t collection_id = ss->GetCollectionId();
                status = mem_mgr_->Flush(collection_id);
                if (!status.ok()) {
                    return status;
                }

                std::set<std::string> flushed_collections;
                flushed_collections.insert(record.collection_id);
                StartMergeTask(flushed_collections);

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
                    status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, id);
                    if (!status.ok()) {
                        LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "flush", 0) << "Get snapshot fail: " << status.message();
                        return status;
                    }

                    flushed_collections.insert(ss->GetName());
                }

                StartMergeTask(flushed_collections);
            }
            break;
        }

        default:
            break;
    }

    return status;
}

void
DBImpl::StartMergeTask(const std::set<std::string>& collection_names, bool force_merge_all) {
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
                merge_thread_pool_.enqueue(&DBImpl::BackgroundMerge, this, collection_names, force_merge_all));
        }
    }

    // LOG_ENGINE_DEBUG_ << "End StartMergeTask";
}

void
DBImpl::BackgroundMerge(std::set<std::string> collection_names, bool force_merge_all) {
    // LOG_ENGINE_TRACE_ << " Background merge thread start";

    for (auto& collection_name : collection_names) {
        const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

        auto status = merge_mgr_ptr_->MergeFiles(collection_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_name
                              << " reason:" << status.message();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection: " << collection_name;
            break;
        }
    }
}

void
DBImpl::WaitMergeFileFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitMergeFileFinish";
    std::lock_guard<std::mutex> lck(merge_result_mutex_);
    for (auto& iter : merge_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitMergeFileFinish";
}

void
DBImpl::SuspendIfFirst() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (++live_search_num_ == 1) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuilderSuspend();
    }
}

void
DBImpl::ResumeIfLast() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (--live_search_num_ == 0) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuildResume();
    }
}

void
DBImpl::ConfigUpdate(const std::string& name) {
    if (name == "storage.auto_flush_interval") {
        options_.auto_flush_interval_ = config.storage.auto_flush_interval();
    }
}

}  // namespace engine
}  // namespace milvus
