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
#include "server/ValidationUtil.h"
#include "utils/Exception.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"

#include <fiu/fiu-local.h>
#include <src/scheduler/job/BuildIndexJob.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <unordered_set>
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

    DBImpl::Start();
}

DBImpl::~DBImpl() {
    ConfigMgr::GetInstance().Detach("storage.auto_flush_interval", this);

    DBImpl::Stop();
}

////////////////////////////////////////////////////////////////////////////////
// External APIs
////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

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
        InternalFlush("", false);

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
    }
    ctx.collection->SetParams(params);

    // check uid existence
    snapshot::FieldPtr uid_field;
    for (auto& pair : ctx.fields_schema) {
        if (pair.first->GetName() == FIELD_UID) {
            uid_field = pair.first;
            break;
        }
    }

    // add uid field if not specified
    if (uid_field == nullptr) {
        uid_field = std::make_shared<snapshot::Field>(FIELD_UID, 0, DataType::INT64);
    }

    // define uid elements
    auto bloom_filter_element = std::make_shared<snapshot::FieldElement>(
        0, 0, ELEMENT_BLOOM_FILTER, milvus::engine::FieldElementType::FET_BLOOM_FILTER);
    auto delete_doc_element = std::make_shared<snapshot::FieldElement>(
        0, 0, ELEMENT_DELETED_DOCS, milvus::engine::FieldElementType::FET_DELETED_DOCS);
    ctx.fields_schema[uid_field] = {bloom_filter_element, delete_doc_element};

    auto op = std::make_shared<snapshot::CreateCollectionOperation>(ctx);
    return op->Push();
}

Status
DBImpl::DropCollection(const std::string& collection_name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Prepare to drop collection " << collection_name;

    snapshot::ScopedSnapshotT ss;
    auto& snapshots = snapshot::Snapshots::GetInstance();
    STATUS_CHECK(snapshots.GetSnapshot(ss, collection_name));

    // erase insert buffer of this collection
    mem_mgr_->EraseMem(ss->GetCollectionId());

    // erase cache
    ClearCollectionCache(ss, options_.meta_.path_);

    // clear index failed retry map of this collection
    ClearIndexFailedRecord(collection_name);

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

    auto partition = ss->GetPartition(partition_name);
    if (partition != nullptr) {
        // erase insert buffer of this partition
        mem_mgr_->EraseMem(ss->GetCollectionId(), partition->GetID());

        // erase cache
        ClearPartitionCache(ss, options_.meta_.path_, partition->GetID());
    }

    snapshot::PartitionContext context;
    context.name = partition_name;
    auto op = std::make_shared<snapshot::DropPartitionOperation>(context, ss);
    return op->Push();
}

Status
DBImpl::HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(server::ValidatePartitionTags({partition_tag}));
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

    // step 2: compare old index and new index, drop old index, set new index
    CollectionIndex new_index = index;
    CollectionIndex old_index;
    STATUS_CHECK(GetSnapshotIndex(collection_name, field_name, old_index));

    if (!utils::IsSameIndex(old_index, new_index)) {
        DropIndex(collection_name, field_name);
        WaitMergeFileFinish();  // let merge file thread finish since DropIndex start a merge task

        // create field element for new index
        status = SetSnapshotIndex(collection_name, field_name, new_index);
        if (!status.ok()) {
            return status;
        }
    }

    // step 3: merge segments before create index, since there could be some small segments just flushed
    {
        snapshot::ScopedSnapshotT latest_ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name));
        std::set<int64_t> collection_ids = {latest_ss->GetCollectionId()};
        StartMergeTask(collection_ids, true);  // start force-merge task
        WaitMergeFileFinish();                 // let force-merge file thread finish
    }

    // clear index failed retry map of this collection
    ClearIndexFailedRecord(collection_name);

    // step 4: iterate segments need to be build index, wait until all segments are built
    while (true) {
        // start background build index thread
        std::vector<std::string> collection_names = {collection_name};
        StartBuildIndexTask(collection_names, true);

        // check if all segments are built
        SnapshotVisitor ss_visitor(collection_name);
        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex(field_name, segment_ids, true);
        if (segment_ids.empty()) {
            break;  // all segments build index finished
        }

        IgnoreIndexFailedSegments(collection_name, segment_ids);
        if (segment_ids.empty()) {
            break;  // some segments failed to build index, and ignored
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

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    ClearIndexCache(ss, options_.meta_.path_, field_name);

    std::set<int64_t> collection_ids = {ss->GetCollectionId()};
    StartMergeTask(collection_ids, true);

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
DBImpl::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
               idx_t op_id) {
    CHECK_INITIALIZED;

    if (data_chunk == nullptr) {
        return Status(DB_ERROR, "Null pointer");
    }

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto partition = ss->GetPartition(partition_name);
    if (partition == nullptr) {
        return Status(DB_NOT_FOUND, "Fail to get partition " + partition_name);
    }

    auto id_field = ss->GetField(FIELD_UID);
    if (id_field == nullptr) {
        return Status(DB_ERROR, "Field '_id' not found");
    }

    // check field names
    auto field_names = ss->GetFieldNames();
    std::unordered_set<std::string> collection_field_names;
    for (auto& name : field_names) {
        collection_field_names.insert(name);
    }
    collection_field_names.erase(engine::FIELD_UID);

    std::unordered_set<std::string> chunk_field_names;
    for (auto& pair : data_chunk->fixed_fields_) {
        chunk_field_names.insert(pair.first);
    }
    for (auto& pair : data_chunk->variable_fields_) {
        chunk_field_names.insert(pair.first);
    }
    chunk_field_names.erase(engine::FIELD_UID);

    if (collection_field_names.size() != chunk_field_names.size()) {
        std::string msg = "Collection has " + std::to_string(collection_field_names.size()) +
                          " fields while the insert data has " + std::to_string(chunk_field_names.size()) + " fields";
        return Status(DB_ERROR, msg);
    } else {
        for (auto& name : chunk_field_names) {
            if (collection_field_names.find(name) == collection_field_names.end()) {
                std::string msg = "The field " + name + " is not defined in collection mapping";
                return Status(DB_ERROR, msg);
            }
        }
    }

    // check id field existence
    auto& params = ss->GetCollection()->GetParams();
    bool auto_genid = true;
    if (params.find(PARAM_UID_AUTOGEN) != params.end()) {
        auto_genid = params[PARAM_UID_AUTOGEN];
    }

    FIXEDX_FIELD_MAP& fields = data_chunk->fixed_fields_;
    auto pair = fields.find(engine::FIELD_UID);
    if (auto_genid) {
        // id is auto generated, but client provides id, return error
        if (pair != fields.end() && pair->second != nullptr) {
            return Status(DB_ERROR, "Field '_id' is auto increment, no need to provide id");
        }
    } else {
        // id is not auto generated, but client doesn't provide id, return error
        if (pair == fields.end() || pair->second == nullptr) {
            return Status(DB_ERROR, "Field '_id' is user defined");
        }
    }

    // consume the data chunk
    DataChunkPtr consume_chunk = std::make_shared<DataChunk>();
    consume_chunk->count_ = data_chunk->count_;
    consume_chunk->fixed_fields_.swap(data_chunk->fixed_fields_);
    consume_chunk->variable_fields_.swap(data_chunk->variable_fields_);

    // generate id
    if (auto_genid) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        IDNumbers ids;
        STATUS_CHECK(id_generator.GetNextIDNumbers(consume_chunk->count_, ids));
        BinaryDataPtr id_data = std::make_shared<BinaryData>();
        id_data->data_.resize(ids.size() * sizeof(int64_t));
        memcpy(id_data->data_.data(), ids.data(), ids.size() * sizeof(int64_t));
        consume_chunk->fixed_fields_[engine::FIELD_UID] = id_data;
        data_chunk->fixed_fields_[engine::FIELD_UID] = id_data;  // return generated id to customer;
    } else {
        BinaryDataPtr id_data = std::make_shared<BinaryData>();
        id_data->data_ = consume_chunk->fixed_fields_[engine::FIELD_UID]->data_;
        data_chunk->fixed_fields_[engine::FIELD_UID] = id_data;  // return the id created by client
    }

    // do insert
    int64_t segment_row_count = 0;
    GetSegmentRowCount(ss->GetCollection(), segment_row_count);

    int64_t collection_id = ss->GetCollectionId();
    int64_t partition_id = partition->GetID();

    std::vector<DataChunkPtr> chunks;
    STATUS_CHECK(utils::SplitChunk(consume_chunk, segment_row_count, chunks));

    for (auto& chunk : chunks) {
        auto status = mem_mgr_->InsertEntities(collection_id, partition_id, chunk, op_id);
        if (!status.ok()) {
            return status;
        }

        std::set<int64_t> collection_ids;
        if (mem_mgr_->RequireFlush(collection_ids)) {
            LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] ", "insert", 0) << "Insert buffer size exceeds limit. Force flush";
            InternalFlush();
        }
    }

    // metrics
    Status status = Status::OK();
    milvus::server::CollectInsertMetrics metrics(data_chunk->count_, status);

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
DBImpl::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "delete", 0) << "Get snapshot fail: " << status.message();
        return status;
    }

    status = mem_mgr_->DeleteEntities(ss->GetCollectionId(), entity_ids, op_id);
    if (!status.ok()) {
        return status;
    }

    std::set<int64_t> collection_ids;
    if (mem_mgr_->RequireFlush(collection_ids)) {
        if (collection_ids.find(ss->GetCollectionId()) != collection_ids.end()) {
            LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] ", "delete", 0)
                              << "Delete count in buffer exceeds limit. Force flush";
            InternalFlush(collection_name);
        }
    }

    return Status::OK();
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

    SuspendIfFirst();

    /* put search job to scheduler and wait job finish */
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitFinish();

    ResumeIfLast();

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

    // remove delete id from the id list
    segment::DeletedDocsPtr deleted_docs_ptr;
    STATUS_CHECK(segment_reader->LoadDeletedDocs(deleted_docs_ptr));
    if (deleted_docs_ptr) {
        const std::vector<offset_t>& delete_ids = deleted_docs_ptr->GetDeletedDocs();
        std::vector<offset_t> temp_ids;
        temp_ids.reserve(delete_ids.size());
        std::copy(delete_ids.begin(), delete_ids.end(), std::back_inserter(temp_ids));
        std::sort(temp_ids.begin(), temp_ids.end(), std::greater<>());
        for (auto offset : temp_ids) {
            entity_ids.erase(entity_ids.begin() + offset, entity_ids.begin() + offset + 1);
        }
    }

    return Status::OK();
}

Status
DBImpl::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                       const std::vector<std::string>& field_names, bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<LoadCollectionHandler>(nullptr, ss, options_.meta_.path_, field_names, force);
    handler->Iterate();
    STATUS_CHECK(handler->GetStatus());

    return Status::OK();
}

Status
DBImpl::Flush(const std::string& collection_name) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    bool has_collection = false;
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
    bool has_collection = false;
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
            snapshot::OperationContext drop_seg_context;
            auto seg = latest_ss->GetResource<snapshot::Segment>(segment_id);
            drop_seg_context.prev_segment = seg;
            auto drop_op = std::make_shared<snapshot::DropSegmentOperation>(drop_seg_context, latest_ss);
            status = drop_op->Push();
            if (!status.ok()) {
                LOG_ENGINE_ERROR_ << "Compact failed for segment " << segment_reader->GetSegmentPath() << ": "
                                  << status.message();
            }
            continue;
        }

        auto deleted_count = deleted_docs->GetCount();
        if (double(deleted_count) / (row_count + deleted_count) < threshold) {
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
DBImpl::InternalFlush(const std::string& collection_name, bool merge) {
    Status status;
    std::set<int64_t> flushed_collection_ids;
    if (!collection_name.empty()) {
        // flush one collection
        snapshot::ScopedSnapshotT ss;
        status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
        if (!status.ok()) {
            LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "flush", 0) << "Get snapshot fail: " << status.message();
            return;
        }

        {
            const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
            int64_t collection_id = ss->GetCollectionId();
            status = mem_mgr_->Flush(collection_id);
            if (!status.ok()) {
                return;
            }
            flushed_collection_ids.insert(collection_id);
        }
    } else {
        // flush all collections
        {
            const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
            status = mem_mgr_->Flush(flushed_collection_ids);
            if (!status.ok()) {
                return;
            }
        }
    }

    if (merge) {
        StartMergeTask(flushed_collection_ids, false);
    }
}

void
DBImpl::TimingFlushThread() {
    SetThreadName("timing_flush");
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
    SetThreadName("timing_metric");
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
DBImpl::StartBuildIndexTask(const std::vector<std::string>& collection_names, bool force_build) {
    if (collection_names.empty()) {
        return;  // no need to start thread
    }

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
                index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndexTask, this, collection_names, force_build));
        }
    }
}

void
DBImpl::BackgroundBuildIndexTask(std::vector<std::string> collection_names, bool force_build) {
    SetThreadName("build_index");

    std::unique_lock<std::mutex> lock(build_index_mutex_);

    for (const auto& collection_name : collection_names) {
        snapshot::ScopedSnapshotT latest_ss;
        auto status = snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        if (!status.ok()) {
            return;
        }
        SnapshotVisitor ss_visitor(latest_ss);

        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex("", segment_ids, force_build);
        if (segment_ids.empty()) {
            continue;
        }

        // check index retry times
        IgnoreIndexFailedSegments(collection_name, segment_ids);
        if (segment_ids.empty()) {
            continue;
        }

        // start build index job
        LOG_ENGINE_DEBUG_ << "Create BuildIndexJob for " << segment_ids.size() << " segments of " << collection_name;
        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info before build index
        scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(latest_ss, options_, segment_ids);

        IncreaseLiveBuildTaskNum();
        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitFinish();
        DecreaseLiveBuildTaskNum();

        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info after build index

        // record failed segments, avoid build index hang
        snapshot::IDS_TYPE& failed_ids = job->FailedSegments();
        MarkIndexFailedSegments(collection_name, failed_ids);

        if (!job->status().ok()) {
            LOG_ENGINE_ERROR_ << job->status().message();
        }
    }
}

void
DBImpl::TimingIndexThread() {
    SetThreadName("timing_index");
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
        StartBuildIndexTask(collection_names, false);
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
DBImpl::StartMergeTask(const std::set<int64_t>& collection_ids, bool force_merge_all) {
    if (collection_ids.empty()) {
        return;  // no need to start thread
    }

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
                merge_thread_pool_.enqueue(&DBImpl::BackgroundMerge, this, collection_ids, force_merge_all));
        }
    }
}

void
DBImpl::BackgroundMerge(std::set<int64_t> collection_ids, bool force_merge_all) {
    SetThreadName("merge");

    for (auto& collection_id : collection_ids) {
        const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

        MergeStrategyType type = force_merge_all ? MergeStrategyType::ADAPTIVE : MergeStrategyType::LAYERED;
        auto status = merge_mgr_ptr_->MergeSegments(collection_id, type);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get merge files for collection id: " << collection_id
                              << " reason:" << status.message();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection id: " << collection_id;
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
DBImpl::IncreaseLiveBuildTaskNum() {
    std::lock_guard<std::mutex> lock(live_build_count_mutex_);
    ++live_build_num_;
}

void
DBImpl::DecreaseLiveBuildTaskNum() {
    std::lock_guard<std::mutex> lock(live_build_count_mutex_);
    --live_build_num_;
}

bool
DBImpl::IsBuildingIndex() {
    std::lock_guard<std::mutex> lock(live_build_count_mutex_);
    return live_build_num_ > 0;
}

void
DBImpl::ConfigUpdate(const std::string& name) {
    if (name == "storage.auto_flush_interval") {
        options_.auto_flush_interval_ = config.storage.auto_flush_interval();
    }
}

void
DBImpl::MarkIndexFailedSegments(const std::string& collection_name, const snapshot::IDS_TYPE& failed_ids) {
    std::lock_guard<std::mutex> lock(index_retry_mutex_);
    SegmentIndexRetryMap& retry_map = index_retry_map_[collection_name];
    for (auto& id : failed_ids) {
        retry_map[id]++;
    }
}

void
DBImpl::IgnoreIndexFailedSegments(const std::string& collection_name, snapshot::IDS_TYPE& segment_ids) {
    std::lock_guard<std::mutex> lock(index_retry_mutex_);
    SegmentIndexRetryMap& retry_map = index_retry_map_[collection_name];
    snapshot::IDS_TYPE segment_ids_to_build;
    for (auto id : segment_ids) {
        if (retry_map[id] < BUILD_INEDX_RETRY_TIMES) {
            segment_ids_to_build.push_back(id);
        }
    }
    segment_ids.swap(segment_ids_to_build);
}

void
DBImpl::ClearIndexFailedRecord(const std::string& collection_name) {
    std::lock_guard<std::mutex> lock(index_retry_mutex_);
    index_retry_map_.erase(collection_name);
}

}  // namespace engine
}  // namespace milvus
