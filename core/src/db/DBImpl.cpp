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
<<<<<<< HEAD
#include "db/merge/MergeTask.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshots.h"
=======
#include "engine/EngineFactory.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/BuilderSuspend.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/FaissIO.h"
#include "index/thirdparty/faiss/utils/distances.h"
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

<<<<<<< HEAD
    /* watch on storage.auto_flush_interval */
    ConfigMgr::GetInstance().Attach("storage.auto_flush_interval", this);
=======
    SetIdentity("DBImpl");
    AddCacheInsertDataListener();
    AddUseBlasThresholdListener();
    knowhere::enable_faiss_logging();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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

<<<<<<< HEAD
    // TODO: merge files
=======
    // server may be closed unexpected, these un-merge files need to be merged when server restart
    // and soft-delete files need to be deleted when server restart
    std::set<std::string> merge_collection_ids;
    std::vector<meta::CollectionSchema> collection_schema_array;
    meta_ptr_->AllCollections(collection_schema_array);
    for (auto& schema : collection_schema_array) {
        merge_collection_ids.insert(schema.collection_id_);
    }
    StartMergeTask(merge_collection_ids, true);

    // wal
    if (options_.wal_enable_) {
        auto error_code = DB_ERROR;
        if (wal_mgr_ != nullptr) {
            error_code = wal_mgr_->Init(meta_ptr_);
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
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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
<<<<<<< HEAD
        bg_metric_thread_ = std::thread(&DBImpl::TimingMetricThread, this);
=======
        bg_metric_thread_ = std::thread(&DBImpl::BackgroundMetricThread, this);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

    return snapshots.DropCollection(ss->GetCollectionId(), std::numeric_limits<snapshot::LSN_TYPE>::max());
}

Status
DBImpl::HasCollection(const std::string& collection_name, bool& has_or_not) {
    CHECK_INITIALIZED;

<<<<<<< HEAD
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    has_or_not = status.ok();

=======
    // dates partly delete files of the collection but currently we don't support
    LOG_ENGINE_DEBUG_ << "Prepare to delete collection " << collection_id;

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DropCollection(collection_id);
    }

    status = mem_mgr_->EraseMemVector(collection_id);      // not allow insert
    status = meta_ptr_->DropCollections({collection_id});  // soft delete collection
    index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    std::vector<std::string> partition_id_array;
    for (auto& schema : partition_array) {
        if (options_.wal_enable_) {
            wal_mgr_->DropCollection(schema.collection_id_);
        }
        status = mem_mgr_->EraseMemVector(schema.collection_id_);
        index_failed_checker_.CleanFailedIndexFileOfCollection(schema.collection_id_);
        partition_id_array.push_back(schema.collection_id_);
    }

    status = meta_ptr_->DropCollections(partition_id_array);
    fiu_do_on("DBImpl.DropCollection.failed", status = Status(DB_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

    // erase insert buffer of this partition
    auto partition = ss->GetPartition(partition_name);
    if (partition != nullptr) {
        mem_mgr_->EraseMem(ss->GetCollectionId(), partition->GetID());
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
<<<<<<< HEAD
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
=======
DBImpl::PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                          bool force) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: get all collection files from parent collection
    meta::FilesHolder files_holder;
#if 0
    auto status = meta_ptr_->FilesToSearch(collection_id, files_holder);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    if (!status.ok()) {
        return status;
    }

<<<<<<< HEAD
    // step 5: start background build index thread
    std::vector<std::string> collection_names = {collection_name};
    WaitBuildIndexFinish();
    StartBuildIndexTask(collection_names, true);
=======
    // step 2: get files from partition collections
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
    }
#else
    auto status = meta_ptr_->FilesToSearch(collection_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);

    std::set<std::string> partition_ids;
    for (auto& schema : partition_array) {
        partition_ids.insert(schema.collection_id_);
    }

    status = meta_ptr_->FilesToSearchEx(collection_id, partition_ids, files_holder);
    if (!status.ok()) {
        return status;
    }
#endif

    int64_t size = 0;
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t available_size = cache_total - cache_usage;

    // step 3: load file one by one
    milvus::engine::meta::SegmentsSchema& files_array = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << "Begin pre-load collection:" + collection_id + ", totally " << files_array.size()
                      << " files need to be pre-loaded";
    TimeRecorderAuto rc("Pre-load collection:" + collection_id);
    for (auto& file : files_array) {
        // client break the connection, no need to continue
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, stop load collection";
            break;
        }

        EngineType engine_type;
        if (file.file_type_ == meta::SegmentSchema::FILE_TYPE::RAW ||
            file.file_type_ == meta::SegmentSchema::FILE_TYPE::TO_INDEX ||
            file.file_type_ == meta::SegmentSchema::FILE_TYPE::BACKUP) {
            engine_type =
                utils::IsBinaryMetricType(file.metric_type_) ? EngineType::FAISS_BIN_IDMAP : EngineType::FAISS_IDMAP;
        } else {
            engine_type = (EngineType)file.engine_type_;
        }
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    // step 6: iterate segments need to be build index, wait until all segments are built
    while (true) {
        SnapshotVisitor ss_visitor(collection_name);
        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex(field_name, segment_ids);
        if (segment_ids.empty()) {
            break;  // all segments build index finished
        }

        snapshot::ScopedSnapshotT ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
        IgnoreIndexFailedSegments(ss->GetCollectionId(), segment_ids);
        if (segment_ids.empty()) {
            break;  // some segments failed to build index, and ignored
        }

        index_req_swn_.Wait_For(std::chrono::seconds(1));

<<<<<<< HEAD
        // client break the connection, no need to block, check every 1 second
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, build index in background";
            break;  // just break, not return, continue to update partitions files to to_index
=======
            size += engine->Size();
            if (!force && size > available_size) {
                LOG_ENGINE_DEBUG_ << "Pre-load cancelled since cache is almost full";
                return Status(SERVER_CACHE_FULL, "Cache is full");
            }
        } catch (std::exception& ex) {
            std::string msg = "Pre-load collection encounter exception: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        }
    }

    return Status::OK();
}

Status
<<<<<<< HEAD
DBImpl::DropIndex(const std::string& collection_name, const std::string& field_name) {
    CHECK_INITIALIZED;
=======
DBImpl::ReLoadSegmentsDeletedDocs(const std::string& collection_id, const std::vector<int64_t>& segment_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::FilesHolder files_holder;
    std::vector<size_t> file_ids;
    for (auto& id : segment_ids) {
        file_ids.emplace_back(id);
    }

    auto status = meta_ptr_->FilesByID(file_ids, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed get file holders by ids: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    milvus::engine::meta::SegmentsSchema hold_files = files_holder.HoldFiles();

    for (auto& file : hold_files) {
        std::string segment_dir;
        utils::GetParentPath(file.location_, segment_dir);

        auto data_obj_ptr = cache::CpuCacheMgr::GetInstance()->GetIndex(file.location_);
        auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
        if (nullptr == index) {
            LOG_ENGINE_WARNING_ << "Index " << file.location_ << " not found";
            continue;
        }

        segment::SegmentReader segment_reader(segment_dir);

        segment::DeletedDocsPtr delete_docs = std::make_shared<segment::DeletedDocs>();
        segment_reader.LoadDeletedDocs(delete_docs);
        auto& docs_offsets = delete_docs->GetDeletedDocs();

        faiss::ConcurrentBitsetPtr blacklist = index->GetBlacklist();
        if (nullptr == blacklist) {
            LOG_ENGINE_WARNING_ << "Index " << file.location_ << " is empty";
            faiss::ConcurrentBitsetPtr concurrent_bitset_ptr =
                std::make_shared<faiss::ConcurrentBitset>(index->Count());
            index->SetBlacklist(concurrent_bitset_ptr);
            blacklist = concurrent_bitset_ptr;
        }

        for (auto& i : docs_offsets) {
            if (!blacklist->test(i)) {
                blacklist->set(i);
            }
        }
    }

    return Status::OK();
}

Status
DBImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_name << " field: " << field_name;

    STATUS_CHECK(DeleteSnapshotIndex(collection_name, field_name));

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
    std::set<int64_t> collection_ids = {ss->GetCollectionId()};
    StartMergeTask(collection_ids, true);

    return Status::OK();
}

Status
DBImpl::DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Describe index for collection: " << collection_name << " field: " << field_name;

<<<<<<< HEAD
    STATUS_CHECK(GetSnapshotIndex(collection_name, field_name, index));

    return Status::OK();
=======
    uint64_t lsn = 0;
    if (options_.wal_enable_) {
        lsn = wal_mgr_->CreatePartition(collection_id, partition_tag);
    } else {
        meta_ptr_->GetCollectionFlushLSN(collection_id, lsn);
    }
    return meta_ptr_->CreatePartition(collection_id, partition_name, partition_tag, lsn);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

<<<<<<< HEAD
    // consume the data chunk
    DataChunkPtr consume_chunk = std::make_shared<DataChunk>();
    consume_chunk->count_ = data_chunk->count_;
    consume_chunk->fixed_fields_.swap(data_chunk->fixed_fields_);
    consume_chunk->variable_fields_.swap(data_chunk->variable_fields_);
=======
    if (options_.wal_enable_) {
        wal_mgr_->DropPartition(collection_id, partition_tag);
    }

    return DropPartition(partition_name);
}
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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
    int64_t segment_row_count = DEFAULT_SEGMENT_ROW_COUNT;
    if (params.find(PARAM_SEGMENT_ROW_COUNT) != params.end()) {
        segment_row_count = params[PARAM_SEGMENT_ROW_COUNT];
    }

    int64_t collection_id = ss->GetCollectionId();
    int64_t partition_id = partition->GetID();

    std::vector<DataChunkPtr> chunks;
    STATUS_CHECK(utils::SplitChunk(consume_chunk, segment_row_count, chunks));

    for (auto& chunk : chunks) {
        auto status = mem_mgr_->InsertEntities(collection_id, partition_id, chunk, op_id);
        if (!status.ok()) {
            return status;
        }
        if (mem_mgr_->GetCurrentMem() > options_.insert_buffer_size_) {
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
<<<<<<< HEAD
    bool has_collection = false;
    status = HasCollection(collection_name, has_collection);
=======
    if (options_.wal_enable_) {
        wal_mgr_->DeleteById(collection_id, vector_ids);
        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.type = wal::MXLogType::Delete;
        record.collection_id = collection_id;
        record.ids = vector_ids.data();
        record.length = vector_ids.size();

        status = ExecWalRecord(record);
    }

    return status;
}

Status
DBImpl::Flush(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    bool has_collection;
    status = HasCollection(collection_id, has_collection);
    if (!status.ok()) {
        return status;
    }
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection to flush does not exist: " << collection_id;
        return Status(DB_NOT_FOUND, "Collection to flush does not exist");
    }

    LOG_ENGINE_DEBUG_ << "Begin flush collection: " << collection_id;

    if (options_.wal_enable_) {
        LOG_ENGINE_DEBUG_ << "WAL flush";
        auto lsn = wal_mgr_->Flush(collection_id);
        if (lsn != 0) {
            swn_wal_.Notify();
            flush_req_swn_.Wait();
        } else {
            // no collection flushed, call merge task to cleanup files
            std::set<std::string> merge_collection_ids;
            StartMergeTask(merge_collection_ids);
        }
    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush(collection_id);
    }

    LOG_ENGINE_DEBUG_ << "End flush collection: " << collection_id;

    return status;
}

Status
DBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Begin flush all collections";

    Status status;
    fiu_do_on("options_wal_enable_false", options_.wal_enable_ = false);
    if (options_.wal_enable_) {
        LOG_ENGINE_DEBUG_ << "WAL flush";
        auto lsn = wal_mgr_->Flush();
        if (lsn != 0) {
            swn_wal_.Notify();
            flush_req_swn_.Wait();
        } else {
            // no collection flushed, call merge task to cleanup files
            std::set<std::string> merge_collection_ids;
            StartMergeTask(merge_collection_ids);
        }
    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush();
    }

    LOG_ENGINE_DEBUG_ << "End flush all collections";

    return status;
}

Status
DBImpl::Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_id, double threshold) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            LOG_ENGINE_ERROR_ << "Collection to compact does not exist: " << collection_id;
            return Status(DB_NOT_FOUND, "Collection to compact does not exist");
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            LOG_ENGINE_ERROR_ << "Collection to compact does not exist: " << collection_id;
            return Status(DB_NOT_FOUND, "Collection to compact does not exist");
        }
    }

    LOG_ENGINE_DEBUG_ << "Before compacting, wait for build index thread to finish...";

    std::vector<meta::CollectionSchema> collection_array;
    status = meta_ptr_->ShowPartitions(collection_id, collection_array);
    collection_array.push_back(collection_schema);

    const std::lock_guard<std::mutex> index_lock(build_index_mutex_);
    const std::lock_guard<std::mutex> merge_lock(flush_merge_compact_mutex_);

    LOG_ENGINE_DEBUG_ << "Compacting collection: " << collection_id;

    // Get files to compact from meta.
    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};
    meta::FilesHolder files_holder;
    status = meta_ptr_->FilesByTypeEx(collection_array, file_types, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files to compact: " + status.message();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    LOG_ENGINE_DEBUG_ << "Found " << files_holder.HoldFiles().size() << " segment to compact";

    Status compact_status;
    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files_to_compact = files_holder.HoldFiles();
    for (auto iter = files_to_compact.begin(); iter != files_to_compact.end();) {
        // client break the connection, no need to continue
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, stop compact operation";
            break;
        }

        meta::SegmentSchema file = *iter;
        iter = files_to_compact.erase(iter);

        // Check if the segment needs compacting
        std::string segment_dir;
        utils::GetParentPath(file.location_, segment_dir);

        segment::SegmentReader segment_reader(segment_dir);
        size_t deleted_docs_size;
        status = segment_reader.ReadDeletedDocsSize(deleted_docs_size);
        if (!status.ok()) {
            files_holder.UnmarkFile(file);
            continue;  // skip this file and try compact next one
        }

        meta::SegmentsSchema files_to_update;
        if (deleted_docs_size != 0) {
            compact_status = CompactFile(file, threshold, files_to_update);

            if (!compact_status.ok()) {
                LOG_ENGINE_ERROR_ << "Compact failed for segment " << file.segment_id_ << ": "
                                  << compact_status.message();
                files_holder.UnmarkFile(file);
                continue;  // skip this file and try compact next one
            }
        } else {
            files_holder.UnmarkFile(file);
            LOG_ENGINE_DEBUG_ << "Segment " << file.segment_id_ << " has no deleted data. No need to compact";
            continue;  // skip this file and try compact next one
        }

        LOG_ENGINE_DEBUG_ << "Updating meta after compaction...";
        status = meta_ptr_->UpdateCollectionFiles(files_to_update);
        files_holder.UnmarkFile(file);
        if (!status.ok()) {
            compact_status = status;
            break;  // meta error, could not go on
        }
    }

    if (compact_status.ok()) {
        LOG_ENGINE_DEBUG_ << "Finished compacting collection: " << collection_id;
    }

    return compact_status;
}

Status
DBImpl::CompactFile(const meta::SegmentSchema& file, double threshold, meta::SegmentsSchema& files_to_update) {
    LOG_ENGINE_DEBUG_ << "Compacting segment " << file.segment_id_ << " for collection: " << file.collection_id_;

    std::string segment_dir_to_merge;
    utils::GetParentPath(file.location_, segment_dir_to_merge);

    // no need to compact if deleted vectors are too few(less than threashold)
    if (file.row_count_ > 0 && threshold > 0.0) {
        segment::SegmentReader segment_reader_to_merge(segment_dir_to_merge);
        segment::DeletedDocsPtr deleted_docs_ptr;
        auto status = segment_reader_to_merge.LoadDeletedDocs(deleted_docs_ptr);
        if (status.ok()) {
            auto delete_items = deleted_docs_ptr->GetDeletedDocs();
            double delete_rate = (double)delete_items.size() / (double)(delete_items.size() + file.row_count_);
            if (delete_rate < threshold) {
                LOG_ENGINE_DEBUG_ << "Delete rate less than " << threshold << ", no need to compact for"
                                  << segment_dir_to_merge;
                return Status::OK();
            }
        }
    }

    // Create new collection file
    meta::SegmentSchema compacted_file;
    compacted_file.collection_id_ = file.collection_id_;
    compacted_file.file_type_ = meta::SegmentSchema::NEW_MERGE;  // TODO: use NEW_MERGE for now
    auto status = meta_ptr_->CreateCollectionFile(compacted_file);

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to create collection file: " << status.message();
        return status;
    }

    // Compact (merge) file to the newly created collection file
    std::string new_segment_dir;
    utils::GetParentPath(compacted_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    LOG_ENGINE_DEBUG_ << "Compacting begin...";
    segment_writer_ptr->Merge(segment_dir_to_merge, compacted_file.file_id_);

    // Serialize
    LOG_ENGINE_DEBUG_ << "Serializing compacted segment...";
    status = segment_writer_ptr->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize compacted segment: " << status.message();
        compacted_file.file_type_ = meta::SegmentSchema::TO_DELETE;
        auto mark_status = meta_ptr_->UpdateCollectionFile(compacted_file);
        if (mark_status.ok()) {
            LOG_ENGINE_DEBUG_ << "Mark file: " << compacted_file.file_id_ << " to to_delete";
        }

        return status;
    }

    // Update compacted file state, if origin file is backup or to_index, set compacted file to to_index
    compacted_file.file_size_ = segment_writer_ptr->Size();
    compacted_file.row_count_ = segment_writer_ptr->VectorCount();
    if ((file.file_type_ == (int32_t)meta::SegmentSchema::BACKUP ||
         file.file_type_ == (int32_t)meta::SegmentSchema::TO_INDEX) &&
        (compacted_file.row_count_ > meta::BUILD_INDEX_THRESHOLD)) {
        compacted_file.file_type_ = meta::SegmentSchema::TO_INDEX;
    } else {
        compacted_file.file_type_ = meta::SegmentSchema::RAW;
    }

    if (compacted_file.row_count_ == 0) {
        LOG_ENGINE_DEBUG_ << "Compacted segment is empty. Mark it as TO_DELETE";
        compacted_file.file_type_ = meta::SegmentSchema::TO_DELETE;
    }

    files_to_update.emplace_back(compacted_file);

    // Set all files in segment to TO_DELETE
    auto& segment_id = file.segment_id_;
    meta::FilesHolder files_holder;
    status = meta_ptr_->GetCollectionFilesBySegmentId(segment_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& segment_files = files_holder.HoldFiles();
    for (auto& f : segment_files) {
        f.file_type_ = meta::SegmentSchema::FILE_TYPE::TO_DELETE;
        files_to_update.emplace_back(f);
    }
    files_holder.ReleaseFiles();

    LOG_ENGINE_DEBUG_ << "Compacted segment " << compacted_file.segment_id_ << " from "
                      << std::to_string(file.file_size_) << " bytes to " << std::to_string(compacted_file.file_size_)
                      << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

Status
DBImpl::GetVectorsByID(const engine::meta::CollectionSchema& collection, const IDNumbers& id_array,
                       std::vector<engine::VectorsData>& vectors) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::FilesHolder files_holder;
    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};

    std::vector<meta::CollectionSchema> collection_array;
    auto status = meta_ptr_->ShowPartitions(collection.collection_id_, collection_array);

    collection_array.push_back(collection);
    status = meta_ptr_->FilesByTypeEx(collection_array, file_types, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files for GetVectorByID: " + status.message();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    if (files_holder.HoldFiles().empty()) {
        LOG_ENGINE_DEBUG_ << "No files to get vector by id from";
        return Status(DB_NOT_FOUND, "Collection is empty");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();
    status = GetVectorsByIdHelper(id_array, vectors, files_holder);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();

    if (vectors.empty()) {
        std::string msg = "Vectors not found in collection " + collection.collection_id_;
        LOG_ENGINE_DEBUG_ << msg;
    }

    return status;
}

Status
DBImpl::GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: check collection existence
    bool has_collection;
    auto status = HasCollection(collection_id, has_collection);
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection " << collection_id << " does not exist: ";
        return Status(DB_NOT_FOUND, "Collection does not exist");
    }
    if (!status.ok()) {
        return status;
    }

    //  step 2: find segment
    meta::FilesHolder files_holder;
    status = meta_ptr_->GetCollectionFilesBySegmentId(segment_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& collection_files = files_holder.HoldFiles();
    if (collection_files.empty()) {
        return Status(DB_NOT_FOUND, "Segment does not exist");
    }

    // check the segment is belong to this collection
    if (collection_files[0].collection_id_ != collection_id) {
        // the segment could be in a partition under this collection
        meta::CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_files[0].collection_id_;
        status = DescribeCollection(collection_schema);
        if (collection_schema.owner_collection_ != collection_id) {
            return Status(DB_NOT_FOUND, "Segment does not belong to this collection");
        }
    }

    // step 3: load segment ids and delete offset
    std::string segment_dir;
    engine::utils::GetParentPath(collection_files[0].location_, segment_dir);
    segment::SegmentReader segment_reader(segment_dir);

    std::vector<segment::doc_id_t> uids;
    status = segment_reader.LoadUids(uids);
    if (!status.ok()) {
        return status;
    }

    segment::DeletedDocsPtr deleted_docs_ptr;
    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
    if (!status.ok()) {
        return status;
    }

    // step 4: construct id array
    // avoid duplicate offset and erase from max offset to min offset
    auto& deleted_offset = deleted_docs_ptr->GetDeletedDocs();
    std::set<segment::offset_t, std::greater<segment::offset_t>> ordered_offset;
    for (segment::offset_t offset : deleted_offset) {
        ordered_offset.insert(offset);
    }
    for (segment::offset_t offset : ordered_offset) {
        uids.erase(uids.begin() + offset);
    }
    vector_ids.swap(uids);

    return status;
}

Status
DBImpl::GetVectorsByIdHelper(const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors,
                             meta::FilesHolder& files_holder) {
    // attention: this is a copy, not a reference, since the files_holder.UnMarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << "Getting vector by id in " << files.size() << " files, id count = " << id_array.size();

    // sometimes not all of id_array can be found, we need to return empty vector for id not found
    // for example:
    // id_array = [1, -1, 2, -1, 3]
    // vectors should return [valid_vector, empty_vector, valid_vector, empty_vector, valid_vector]
    // the ID2RAW is to ensure returned vector sequence is consist with id_array
    using ID2VECTOR = std::map<int64_t, VectorsData>;
    ID2VECTOR map_id2vector;

    vectors.clear();

    IDNumbers temp_ids = id_array;
    for (auto& file : files) {
        if (temp_ids.empty()) {
            break;  // all vectors found, no need to continue
        }
        // Load bloom filter
        std::string segment_dir;
        engine::utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        auto status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            return status;
        }

        for (IDNumbers::iterator it = temp_ids.begin(); it != temp_ids.end();) {
            int64_t vector_id = *it;
            // each id must has a VectorsData
            // if vector not found for an id, its VectorsData's vector_count = 0, else 1
            VectorsData& vector_ref = map_id2vector[vector_id];

            // Check if the id is present in bloom filter.
            if (id_bloom_filter_ptr->Check(vector_id)) {
                // Load uids and check if the id is indeed present. If yes, find its offset.
                std::vector<segment::doc_id_t> uids;
                auto status = segment_reader.LoadUids(uids);
                if (!status.ok()) {
                    return status;
                }

                auto found = std::find(uids.begin(), uids.end(), vector_id);
                if (found != uids.end()) {
                    auto offset = std::distance(uids.begin(), found);

                    // Check whether the id has been deleted
                    segment::DeletedDocsPtr deleted_docs_ptr;
                    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
                    if (!status.ok()) {
                        LOG_ENGINE_ERROR_ << status.message();
                        return status;
                    }
                    auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

                    auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
                    if (deleted == deleted_docs.end()) {
                        // Load raw vector
                        bool is_binary = utils::IsBinaryMetricType(file.metric_type_);
                        size_t single_vector_bytes = is_binary ? file.dimension_ / 8 : file.dimension_ * sizeof(float);
                        std::vector<uint8_t> raw_vector;
                        status =
                            segment_reader.LoadVectors(offset * single_vector_bytes, single_vector_bytes, raw_vector);
                        if (!status.ok()) {
                            LOG_ENGINE_ERROR_ << status.message();
                            return status;
                        }

                        vector_ref.vector_count_ = 1;
                        if (is_binary) {
                            vector_ref.binary_data_.swap(raw_vector);
                        } else {
                            std::vector<float> float_vector;
                            float_vector.resize(file.dimension_);
                            memcpy(float_vector.data(), raw_vector.data(), single_vector_bytes);
                            vector_ref.float_data_.swap(float_vector);
                        }
                        temp_ids.erase(it);
                        continue;
                    }
                }
            }

            it++;
        }

        // unmark file, allow the file to be deleted
        files_holder.UnmarkFile(file);
    }

    for (auto id : id_array) {
        VectorsData& vector_ref = map_id2vector[id];

        VectorsData data;
        data.vector_count_ = vector_ref.vector_count_;
        if (data.vector_count_ > 0) {
            data.float_data_ = vector_ref.float_data_;    // copy data since there could be duplicated id
            data.binary_data_ = vector_ref.binary_data_;  // copy data since there could be duplicated id
        }
        vectors.emplace_back(data);
    }

    return Status::OK();
}

Status
DBImpl::CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                    const CollectionIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: wait merge file thread finished to avoid duplicate data bug
    auto status = Flush();
    WaitMergeFileFinish();  // let merge file thread finish

    // merge all files for this collection, including its partitions
    std::set<std::string> merge_collection_ids = {collection_id};
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        merge_collection_ids.insert(schema.collection_id_);
    }
    StartMergeTask(merge_collection_ids, true);  // start force-merge task
    WaitMergeFileFinish();                       // let force-merge file thread finish

    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);

        // step 2: check index difference
        CollectionIndex old_index;
        status = DescribeIndex(collection_id, old_index);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get collection index info for collection: " << collection_id;
            return status;
        }

        // step 3: update index info
        CollectionIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateCollection
        if (!utils::IsSameIndex(old_index, new_index)) {
            status = UpdateCollectionIndexRecursively(collection_id, new_index);
            if (!status.ok()) {
                return status;
            }
        }
    }

    // step 4: wait and build index
    status = index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);
    status = WaitCollectionIndexRecursively(context, collection_id, index);

    return status;
}

Status
DBImpl::DescribeIndex(const std::string& collection_id, CollectionIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->DescribeCollectionIndex(collection_id, index);
}

Status
DBImpl::DropIndex(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_id;
    auto status = DropCollectionIndexRecursively(collection_id);
    std::set<std::string> merge_collection_ids = {collection_id};
    StartMergeTask(merge_collection_ids, true);  // merge small files after drop index
    return status;
}

Status
DBImpl::QueryByIDs(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                   const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
                   const IDNumbers& id_array, ResultIds& result_ids, ResultDistances& result_distances) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    if (id_array.empty()) {
        return Status(DB_ERROR, "Empty id array during query by id");
    }

    TimeRecorder rc("Query by id in collection:" + collection_id);

    // get collection schema
    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            std::string msg = "Collection to search does not exist: " + collection_id;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_NOT_FOUND, msg);
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            std::string msg = "Collection to search does not exist: " + collection_id;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_NOT_FOUND, msg);
        }
    }

    rc.RecordSection("get collection schema");

    // get target vectors data
    std::vector<milvus::engine::VectorsData> vectors;
    status = GetVectorsByID(collection_schema, id_array, vectors);
    if (!status.ok()) {
        std::string msg = "Failed to get vector data for collection: " + collection_id;
        LOG_ENGINE_ERROR_ << msg;
        return status;
    }

    // some vectors could not be found, no need to search them
    uint64_t valid_count = 0;
    bool is_binary = utils::IsBinaryMetricType(collection_schema.metric_type_);
    for (auto& vector : vectors) {
        if (vector.vector_count_ > 0) {
            valid_count++;
        }
    }

    // copy valid vectors data for search input
    uint64_t dimension = collection_schema.dimension_;
    VectorsData valid_vectors;
    valid_vectors.vector_count_ = valid_count;
    if (is_binary) {
        valid_vectors.binary_data_.resize(valid_count * dimension / 8);
    } else {
        valid_vectors.float_data_.resize(valid_count * dimension * sizeof(float));
    }

    int64_t valid_index = 0;
    for (size_t i = 0; i < vectors.size(); i++) {
        if (vectors[i].vector_count_ == 0) {
            continue;
        }
        if (is_binary) {
            memcpy(valid_vectors.binary_data_.data() + valid_index * dimension / 8, vectors[i].binary_data_.data(),
                   vectors[i].binary_data_.size());
        } else {
            memcpy(valid_vectors.float_data_.data() + valid_index * dimension, vectors[i].float_data_.data(),
                   vectors[i].float_data_.size() * sizeof(float));
        }
        valid_index++;
    }

    rc.RecordSection("construct query input");

    // search valid vectors
    ResultIds valid_result_ids;
    ResultDistances valid_result_distances;
    status = Query(context, collection_id, partition_tags, k, extra_params, valid_vectors, valid_result_ids,
                   valid_result_distances);
    if (!status.ok()) {
        std::string msg = "Failed to query by id in collection " + collection_id + ", error: " + status.message();
        LOG_ENGINE_ERROR_ << msg;
        return status;
    }

    if (valid_result_ids.size() != valid_count * k || valid_result_distances.size() != valid_count * k) {
        std::string msg = "Failed to query by id in collection " + collection_id + ", result doesn't match id count";
        return Status(DB_ERROR, msg);
    }

    rc.RecordSection("query vealid vectors");

    // construct result
    if (valid_count == id_array.size()) {
        result_ids.swap(valid_result_ids);
        result_distances.swap(valid_result_distances);
    } else {
        result_ids.resize(vectors.size() * k);
        result_distances.resize(vectors.size() * k);
        int64_t valid_index = 0;
        for (uint64_t i = 0; i < vectors.size(); i++) {
            if (vectors[i].vector_count_ > 0) {
                memcpy(result_ids.data() + i * k, valid_result_ids.data() + valid_index * k, k * sizeof(int64_t));
                memcpy(result_distances.data() + i * k, valid_result_distances.data() + valid_index * k,
                       k * sizeof(float));
                valid_index++;
            } else {
                memset(result_ids.data() + i * k, -1, k * sizeof(int64_t));
                for (uint64_t j = i * k; j < i * k + k; j++) {
                    result_distances[j] = std::numeric_limits<float>::max();
                }
            }
        }
    }

    rc.RecordSection("construct result");

    return status;
}

Status
DBImpl::HybridQuery(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                    const std::vector<std::string>& partition_tags,
                    context::HybridSearchContextPtr hybrid_search_context, query::GeneralQueryPtr general_query,
                    std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type, uint64_t& nq,
                    ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    meta::FilesHolder files_holder;
    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole table
        // get all table files from parent table
        status = meta_ptr_->FilesToSearch(collection_id, files_holder);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        if (!status.ok()) {
            return status;
        }
        for (auto& schema : partition_array) {
            status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
            if (!status.ok()) {
                return Status(DB_ERROR, "get files to search failed in HybridQuery");
            }
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    } else {
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        GetPartitionsByTags(collection_id, partition_tags, partition_name_array);

        for (auto& partition_name : partition_name_array) {
            status = meta_ptr_->FilesToSearch(partition_name, files_holder);
            if (!status.ok()) {
                return Status(DB_ERROR, "get files to search failed in HybridQuery");
            }
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = HybridQueryAsync(query_ctx, collection_id, files_holder, hybrid_search_context, general_query, attr_type,
                              nq, result_ids, result_distances);
    if (!status.ok()) {
        return status;
    }
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

    return status;
}

Status
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
              const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
              const VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    meta::FilesHolder files_holder;
    if (partition_tags.empty()) {
#if 0
        // no partition tag specified, means search in whole collection
        // get all collection files from parent collection
        status = meta_ptr_->FilesToSearch(collection_id, files_holder);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        for (auto& schema : partition_array) {
            status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
        }
#else
        // no partition tag specified, means search in whole collection
        // get files from root collection
        status = meta_ptr_->FilesToSearch(collection_id, files_holder);
        if (!status.ok()) {
            return status;
        }

        // get files from partitions
        std::set<std::string> partition_ids;
        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        for (auto& id : partition_array) {
            partition_ids.insert(id.collection_id_);
        }

        status = meta_ptr_->FilesToSearchEx(collection_id, partition_ids, files_holder);
        if (!status.ok()) {
            return status;
        }
#endif

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    } else {
#if 0
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        status = GetPartitionsByTags(collection_id, partition_tags, partition_name_array);
        if (!status.ok()) {
            return status;  // didn't match any partition.
        }

        for (auto& partition_name : partition_name_array) {
            status = meta_ptr_->FilesToSearch(partition_name, files_holder);
        }
#else
        std::set<std::string> partition_name_array;
        status = GetPartitionsByTags(collection_id, partition_tags, partition_name_array);
        if (!status.ok()) {
            return status;  // didn't match any partition.
        }

        std::set<std::string> partition_ids;
        for (auto& partition_name : partition_name_array) {
            partition_ids.insert(partition_name);
        }

        status = meta_ptr_->FilesToSearchEx(collection_id, partition_ids, files_holder);
#endif
        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(tracer.Context(), files_holder, k, extra_params, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    return status;
}

Status
DBImpl::QueryByFileID(const std::shared_ptr<server::Context>& context, const std::vector<std::string>& file_ids,
                      uint64_t k, const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
                      ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query by file id");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // get specified files
    std::vector<size_t> ids;
    for (auto& id : file_ids) {
        std::string::size_type sz;
        ids.push_back(std::stoul(id, &sz));
    }

    meta::FilesHolder files_holder;
    auto status = meta_ptr_->FilesByID(ids, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& search_files = files_holder.HoldFiles();
    if (search_files.empty()) {
        return Status(DB_ERROR, "Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(tracer.Context(), files_holder, k, extra_params, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    return status;
}

Status
DBImpl::Size(uint64_t& result) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->Size(result);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// internal methods
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::QueryAsync(const std::shared_ptr<server::Context>& context, meta::FilesHolder& files_holder, uint64_t k,
                   const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
                   ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query Async");
    server::CollectQueryMetrics metrics(vectors.vector_count_);

    milvus::engine::meta::SegmentsSchema& files = files_holder.HoldFiles();
    if (files.size() > milvus::scheduler::TASK_TABLE_MAX_COUNT) {
        std::string msg =
            "Search files count exceed scheduler limit: " + std::to_string(milvus::scheduler::TASK_TABLE_MAX_COUNT);
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_ERROR, msg);
    }

    TimeRecorder rc("");

    // step 1: construct search job
    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, index file count: %ld", files.size());
    scheduler::SearchJobPtr job = std::make_shared<scheduler::SearchJob>(tracer.Context(), k, extra_params, vectors);
    for (auto& file : files) {
        scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    // Suspend builder
    SuspendIfFirst();

    // step 2: put search job to scheduler and wait result
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();

    // Resume builder
    ResumeIfLast();

    files_holder.ReleaseFiles();
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    // step 3: construct results
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

    return Status::OK();
}

Status
DBImpl::HybridQueryAsync(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                         meta::FilesHolder& files_holder, context::HybridSearchContextPtr hybrid_search_context,
                         query::GeneralQueryPtr general_query,
                         std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type, uint64_t& nq,
                         ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_async_ctx = context->Child("Query Async");

#if 0
    // Construct tasks
    for (auto file : files) {
        std::unordered_map<std::string, engine::DataType> types;
        auto it = attr_type.begin();
        for (; it != attr_type.end(); it++) {
            types.insert(std::make_pair(it->first, (engine::DataType)it->second));
        }

        auto file_ptr = std::make_shared<meta::TableFileSchema>(file);
        search::TaskPtr
            task = std::make_shared<search::Task>(context, file_ptr, general_query, types, hybrid_search_context);
        search::TaskInst::GetInstance().load_queue().push(task);
        search::TaskInst::GetInstance().load_cv().notify_one();
        hybrid_search_context->tasks_.emplace_back(task);
    }

#endif

    //#if 0
    TimeRecorder rc("");

    // step 1: construct search job
    VectorsData vectors;
    milvus::engine::meta::SegmentsSchema& files = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, index file count: %ld", files_holder.HoldFiles().size());
    scheduler::SearchJobPtr job =
        std::make_shared<scheduler::SearchJob>(query_async_ctx, general_query, attr_type, vectors);
    for (auto& file : files) {
        scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    // step 2: put search job to scheduler and wait result
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();

    files_holder.ReleaseFiles();
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    // step 3: construct results
    nq = job->vector_count();
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

    query_async_ctx->GetTraceContext()->GetSpan()->Finish();
    //#endif

    return Status::OK();
}

void
DBImpl::BackgroundIndexThread() {
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
DBImpl::WaitMergeFileFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitMergeFileFinish";
    std::lock_guard<std::mutex> lck(merge_result_mutex_);
    for (auto& iter : merge_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitMergeFileFinish";
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
DBImpl::StartMetricTask() {
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
    uint64_t size;
    Size(size);
    server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
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
DBImpl::StartMergeTask(const std::set<std::string>& merge_collection_ids, bool force_merge_all) {
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
                merge_thread_pool_.enqueue(&DBImpl::BackgroundMerge, this, merge_collection_ids, force_merge_all));
        }
    }

    // LOG_ENGINE_DEBUG_ << "End StartMergeTask";
}

// Status
// DBImpl::MergeHybridFiles(const std::string& collection_id, meta::FilesHolder& files_holder) {
//    // const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
//
//    LOG_ENGINE_DEBUG_ << "Merge files for collection: " << collection_id;
//
//    // step 1: create table file
//    meta::SegmentSchema table_file;
//    table_file.collection_id_ = collection_id;
//    table_file.file_type_ = meta::SegmentSchema::NEW_MERGE;
//    Status status = meta_ptr_->CreateHybridCollectionFile(table_file);
//
//    if (!status.ok()) {
//        LOG_ENGINE_ERROR_ << "Failed to create collection: " << status.ToString();
//        return status;
//    }
//
//    // step 2: merge files
//    /*
//    ExecutionEnginePtr index =
//        EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_,
//                             (MetricType)table_file.metric_type_, table_file.nlist_);
//*/
//    meta::SegmentsSchema updated;
//
//    std::string new_segment_dir;
//    utils::GetParentPath(table_file.location_, new_segment_dir);
//    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);
//
//    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
//    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
//    for (auto& file : files) {
//        server::CollectMergeFilesMetrics metrics;
//        std::string segment_dir_to_merge;
//        utils::GetParentPath(file.location_, segment_dir_to_merge);
//        segment_writer_ptr->Merge(segment_dir_to_merge, table_file.file_id_);
//
//        files_holder.UnmarkFile(file);
//
//        auto file_schema = file;
//        file_schema.file_type_ = meta::SegmentSchema::TO_DELETE;
//        updated.push_back(file_schema);
//        int64_t size = segment_writer_ptr->Size();
//        if (size >= file_schema.index_file_size_) {
//            break;
//        }
//    }
//
//    // step 3: serialize to disk
//    try {
//        status = segment_writer_ptr->Serialize();
//        fiu_do_on("DBImpl.MergeFiles.Serialize_ThrowException", throw std::exception());
//        fiu_do_on("DBImpl.MergeFiles.Serialize_ErrorStatus", status = Status(DB_ERROR, ""));
//    } catch (std::exception& ex) {
//        std::string msg = "Serialize merged index encounter exception: " + std::string(ex.what());
//        LOG_ENGINE_ERROR_ << msg;
//        status = Status(DB_ERROR, msg);
//    }
//
//    if (!status.ok()) {
//        LOG_ENGINE_ERROR_ << "Failed to persist merged segment: " << new_segment_dir << ". Error: " <<
//        status.message();
//
//        // if failed to serialize merge file to disk
//        // typical error: out of disk space, out of memory or permission denied
//        table_file.file_type_ = meta::SegmentSchema::TO_DELETE;
//        status = meta_ptr_->UpdateCollectionFile(table_file);
//        LOG_ENGINE_DEBUG_ << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";
//
//        return status;
//    }
//
//    // step 4: update table files state
//    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
//    // else set file type to RAW, no need to build index
//    if (!utils::IsRawIndexType(table_file.engine_type_)) {
//        table_file.file_type_ = (segment_writer_ptr->Size() >= (size_t)(table_file.index_file_size_))
//                                    ? meta::SegmentSchema::TO_INDEX
//                                    : meta::SegmentSchema::RAW;
//    } else {
//        table_file.file_type_ = meta::SegmentSchema::RAW;
//    }
//    table_file.file_size_ = segment_writer_ptr->Size();
//    table_file.row_count_ = segment_writer_ptr->VectorCount();
//    updated.push_back(table_file);
//    status = meta_ptr_->UpdateCollectionFiles(updated);
//    LOG_ENGINE_DEBUG_ << "New merged segment " << table_file.segment_id_ << " of size " << segment_writer_ptr->Size()
//                      << " bytes";
//
//    if (options_.insert_cache_immediately_) {
//        segment_writer_ptr->Cache();
//    }
//
//    return status;
//}

void
DBImpl::BackgroundMerge(std::set<std::string> collection_ids, bool force_merge_all) {
    // LOG_ENGINE_TRACE_ << " Background merge thread start";

    Status status;
    for (auto& collection_id : collection_ids) {
        const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

        auto old_strategy = merge_mgr_ptr_->Strategy();
        if (force_merge_all) {
            merge_mgr_ptr_->UseStrategy(MergeStrategyType::ADAPTIVE);
        }

        auto status = merge_mgr_ptr_->MergeFiles(collection_id);
        merge_mgr_ptr_->UseStrategy(old_strategy);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_id
                              << " reason:" << status.message();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection: " << collection_id;
            break;
        }
    }

    //    meta_ptr_->Archive();

    {
        uint64_t timeout = (options_.file_cleanup_timeout_ >= 0) ? options_.file_cleanup_timeout_ : 10;
        uint64_t ttl = timeout * meta::SECOND;  // default: file will be hard-deleted few seconds after soft-deleted
        meta_ptr_->CleanUpFilesWithTTL(ttl);
    }

    // LOG_ENGINE_TRACE_ << " Background merge thread exit";
}

void
DBImpl::StartBuildIndexTask() {
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
            index_thread_results_.push_back(index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndex, this));
        }
    }
}

void
DBImpl::BackgroundBuildIndex() {
    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::FilesHolder files_holder;
    meta_ptr_->FilesToIndex(files_holder);

    milvus::engine::meta::SegmentsSchema to_index_files = files_holder.HoldFiles();
    Status status = index_failed_checker_.IgnoreFailedIndexFiles(to_index_files);

    if (!to_index_files.empty()) {
        LOG_ENGINE_DEBUG_ << "Background build index thread begin " << to_index_files.size() << " files";

        // step 2: put build index task to scheduler
        std::vector<std::pair<scheduler::BuildIndexJobPtr, scheduler::SegmentSchemaPtr>> job2file_map;
        for (auto& file : to_index_files) {
            scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(meta_ptr_, options_);
            scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
            job->AddToIndexFiles(file_ptr);
            scheduler::JobMgrInst::GetInstance()->Put(job);
            job2file_map.push_back(std::make_pair(job, file_ptr));
        }

        // step 3: wait build index finished and mark failed files
        int64_t completed = 0;
        for (auto iter = job2file_map.begin(); iter != job2file_map.end(); ++iter) {
            scheduler::BuildIndexJobPtr job = iter->first;
            meta::SegmentSchema& file_schema = *(iter->second.get());
            job->WaitBuildIndexFinish();
            LOG_ENGINE_INFO_ << "Build Index Progress: " << ++completed << " of " << job2file_map.size();
            if (!job->GetStatus().ok()) {
                Status status = job->GetStatus();
                LOG_ENGINE_ERROR_ << "Building index job " << job->id() << " failed: " << status.ToString();

                index_failed_checker_.MarkFailedIndexFile(file_schema, status.message());
            } else {
                LOG_ENGINE_DEBUG_ << "Building index job " << job->id() << " succeed.";

                index_failed_checker_.MarkSucceedIndexFile(file_schema);
            }
            status = files_holder.UnmarkFile(file_schema);
            LOG_ENGINE_DEBUG_ << "Finish build index file " << file_schema.file_id_;
        }

        LOG_ENGINE_DEBUG_ << "Background build index thread finished";
        index_req_swn_.Notify();  // notify CreateIndex check circle
    }
}

Status
DBImpl::GetFilesToBuildIndex(const std::string& collection_id, const std::vector<int>& file_types,
                             meta::FilesHolder& files_holder) {
    files_holder.ReleaseFiles();
    auto status = meta_ptr_->FilesByType(collection_id, file_types, files_holder);

    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    for (const milvus::engine::meta::SegmentSchema& file : files) {
        if (file.file_type_ == static_cast<int>(meta::SegmentSchema::RAW) &&
            file.row_count_ < meta::BUILD_INDEX_THRESHOLD) {
            // skip build index for files that row count less than certain threshold
            files_holder.UnmarkFile(file);
        } else if (index_failed_checker_.IsFailedIndexFile(file)) {
            // skip build index for files that failed before
            files_holder.UnmarkFile(file);
        }
    }

    return Status::OK();
}

Status
DBImpl::GetPartitionByTag(const std::string& collection_id, const std::string& partition_tag,
                          std::string& partition_name) {
    Status status;

    if (partition_tag.empty()) {
        partition_name = collection_id;

    } else {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = partition_tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name = collection_id;
            return status;
        }

        status = meta_ptr_->GetPartitionName(collection_id, partition_tag, partition_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << status.message();
        }
    }

    return status;
}

Status
DBImpl::GetPartitionsByTags(const std::string& collection_id, const std::vector<std::string>& partition_tags,
                            std::set<std::string>& partition_name_array) {
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);

    for (auto& tag : partition_tags) {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name_array.insert(collection_id);
            return status;
        }

        for (auto& schema : partition_array) {
            if (server::StringHelpFunctions::IsRegexMatch(schema.partition_tag_, valid_tag)) {
                partition_name_array.insert(schema.collection_id_);
            }
        }
    }

    if (partition_name_array.empty()) {
        return Status(DB_PARTITION_NOT_FOUND, "The specified partiton does not exist");
    }

    return Status::OK();
}

Status
DBImpl::UpdateCollectionIndexRecursively(const std::string& collection_id, const CollectionIndex& index) {
    DropIndex(collection_id);
    WaitMergeFileFinish();  // DropIndex called StartMergeTask, need to wait merge thread finish
    auto status = meta_ptr_->UpdateCollectionIndex(collection_id, index);
    fiu_do_on("DBImpl.UpdateCollectionIndexRecursively.fail_update_collection_index",
              status = Status(DB_META_TRANSACTION_FAILED, ""));
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to update collection index info for collection: " << collection_id;
        return status;
    }

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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
<<<<<<< HEAD
DBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
=======
DBImpl::WaitCollectionIndexRecursively(const std::shared_ptr<server::Context>& context,
                                       const std::string& collection_id, const CollectionIndex& index) {
    // for IDMAP type, only wait all NEW file converted to RAW file
    // for other type, wait NEW/RAW/NEW_MERGE/NEW_INDEX/TO_INDEX files converted to INDEX files
    std::vector<int> file_types;
    if (utils::IsRawIndexType(index.engine_type_)) {
        file_types = {
            static_cast<int32_t>(meta::SegmentSchema::NEW),
            static_cast<int32_t>(meta::SegmentSchema::NEW_MERGE),
        };
    } else {
        file_types = {
            static_cast<int32_t>(meta::SegmentSchema::RAW),       static_cast<int32_t>(meta::SegmentSchema::NEW),
            static_cast<int32_t>(meta::SegmentSchema::NEW_MERGE), static_cast<int32_t>(meta::SegmentSchema::NEW_INDEX),
            static_cast<int32_t>(meta::SegmentSchema::TO_INDEX),
        };
    }

    // get files to build index
    {
        meta::FilesHolder files_holder;
        auto status = GetFilesToBuildIndex(collection_id, file_types, files_holder);
        int times = 1;
        uint64_t repeat = 0;
        while (!files_holder.HoldFiles().empty()) {
            if (repeat % WAIT_BUILD_INDEX_INTERVAL == 0) {
                LOG_ENGINE_DEBUG_ << files_holder.HoldFiles().size() << " non-index files detected! Will build index "
                                  << times;
                if (!utils::IsRawIndexType(index.engine_type_)) {
                    status = meta_ptr_->UpdateCollectionFilesToIndex(collection_id);
                }
            }

            auto ret = index_req_swn_.Wait_For(std::chrono::seconds(1));
            // client break the connection, no need to block, check every 1 second
            if (context && context->IsConnectionBroken()) {
                LOG_ENGINE_DEBUG_ << "Client connection broken, build index in background";
                break;  // just break, not return, continue to update partitions files to to_index
            }

            // check to_index files every 5 seconds or background index thread finished
            repeat++;
            if (repeat % WAIT_BUILD_INDEX_INTERVAL == 0) {
                GetFilesToBuildIndex(collection_id, file_types, files_holder);
                ++times;
            }
        }
    }

    // build index for partition
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = WaitCollectionIndexRecursively(context, schema.collection_id_, index);
        fiu_do_on("DBImpl.WaitCollectionIndexRecursively.fail_build_collection_Index_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    }

    // failed to build index for some files, return error
    std::string err_msg;
    index_failed_checker_.GetErrMsgForCollection(collection_id, err_msg);
    fiu_do_on("DBImpl.WaitCollectionIndexRecursively.not_empty_err_msg", err_msg.append("fiu"));
    if (!err_msg.empty()) {
        return Status(DB_ERROR, err_msg);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

<<<<<<< HEAD
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
=======
    auto collections_flushed = [&](const std::string collection_id,
                                   const std::set<std::string>& target_collection_names) -> uint64_t {
        uint64_t max_lsn = 0;
        if (options_.wal_enable_ && !target_collection_names.empty()) {
            uint64_t lsn = 0;
            for (auto& collection : target_collection_names) {
                meta_ptr_->GetCollectionFlushLSN(collection, lsn);
                if (lsn > max_lsn) {
                    max_lsn = lsn;
                }
            }
            wal_mgr_->CollectionFlushed(collection_id, lsn);
        }

        std::set<std::string> merge_collection_ids;
        for (auto& collection : target_collection_names) {
            merge_collection_ids.insert(collection);
        }
        StartMergeTask(merge_collection_ids);
        return max_lsn;
    };

    auto force_flush_if_mem_full = [&]() -> uint64_t {
        if (mem_mgr_->GetCurrentMem() > options_.insert_buffer_size_) {
            LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] ", "insert", 0) << "Insert buffer size exceeds limit. Force flush";
            InternalFlush();
        }
    };

    Status status;

    switch (record.type) {
        case wal::MXLogType::Entity: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            status = mem_mgr_->InsertEntities(
                target_collection_name, record.length, record.ids, (record.data_size / record.length / sizeof(float)),
                (const float*)record.data, record.attr_nbytes, record.attr_data_size, record.attr_data, record.lsn);
            force_flush_if_mem_full();

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        }
    }

<<<<<<< HEAD
    return status;
}
=======
            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(uint8_t)),
                                             (const u_int8_t*)record.data, record.lsn);
            force_flush_if_mem_full();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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
<<<<<<< HEAD
            flushed_collection_ids.insert(collection_id);
=======

            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(float)),
                                             (const float*)record.data, record.lsn);
            force_flush_if_mem_full();

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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
        StartMergeTask(flushed_collection_ids);
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

<<<<<<< HEAD
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
=======
                collections_flushed(record.collection_id, flushed_collections);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    server::Metrics::GetInstance().CPUCoreUsagePercentSet();
    server::Metrics::GetInstance().GPUTemperature();
    server::Metrics::GetInstance().CPUTemperature();
    server::Metrics::GetInstance().PushToGateway();
}

<<<<<<< HEAD
void
DBImpl::TimingMetricThread() {
    SetThreadName("timing_metric");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background metric thread exit";
            break;
        }
=======
                uint64_t lsn = collections_flushed("", collection_ids);
                if (options_.wal_enable_) {
                    wal_mgr_->RemoveOldFiles(lsn);
                }
            }
            break;
        }

        default:
            break;
    }
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

        swn_metric_.Wait_For(std::chrono::seconds(BACKGROUND_METRIC_INTERVAL));
        StartMetricTask();
    }
}

void
<<<<<<< HEAD
DBImpl::StartBuildIndexTask(const std::vector<std::string>& collection_names, bool reset_retry_times) {
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
            if (reset_retry_times) {
                std::lock_guard<std::mutex> lock(index_retry_mutex_);
                index_retry_map_.clear();  // reset index retry times
            }

            index_thread_results_.push_back(
                index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndexTask, this, collection_names));
        }
    }
=======
DBImpl::InternalFlush(const std::string& collection_id) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_id;
    ExecWalRecord(record);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}

void
DBImpl::BackgroundBuildIndexTask(std::vector<std::string> collection_names) {
    SetThreadName("build_index");

<<<<<<< HEAD
    std::unique_lock<std::mutex> lock(build_index_mutex_);

    for (const auto& collection_name : collection_names) {
        snapshot::ScopedSnapshotT latest_ss;
        auto status = snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        if (!status.ok()) {
            return;
=======
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
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        }
        SnapshotVisitor ss_visitor(latest_ss);

        snapshot::IDS_TYPE segment_ids;
        ss_visitor.SegmentsToIndex("", segment_ids);
        if (segment_ids.empty()) {
            continue;
        }

        // check index retry times
        snapshot::ID_TYPE collection_id = latest_ss->GetCollectionId();
        IgnoreIndexFailedSegments(collection_id, segment_ids);
        if (segment_ids.empty()) {
            continue;
        }

        // start build index job
        LOG_ENGINE_DEBUG_ << "Create BuildIndexJob for " << segment_ids.size() << " segments of " << collection_name;
        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info before build index
        scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(latest_ss, options_, segment_ids);
        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitFinish();
        cache::CpuCacheMgr::GetInstance().PrintInfo();  // print cache info after build index

        // record failed segments, avoid build index hang
        snapshot::IDS_TYPE& failed_ids = job->FailedSegments();
        MarkIndexFailedSegments(collection_id, failed_ids);

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
<<<<<<< HEAD
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
    // merge task has been finished?
    {
        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        if (!merge_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (merge_thread_results_.back().wait_for(span) == std::future_status::ready) {
                merge_thread_results_.pop_back();
            }
=======
DBImpl::BackgroundMetricThread() {
    SetThreadName("metric_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background metric thread exit";
            break;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

        MergeStrategyType type = force_merge_all ? MergeStrategyType::SIMPLE : MergeStrategyType::LAYERED;
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
DBImpl::ConfigUpdate(const std::string& name) {
    if (name == "storage.auto_flush_interval") {
        options_.auto_flush_interval_ = config.storage.auto_flush_interval();
    }
}

void
DBImpl::MarkIndexFailedSegments(snapshot::ID_TYPE collection_id, const snapshot::IDS_TYPE& failed_ids) {
    std::lock_guard<std::mutex> lock(index_retry_mutex_);
    SegmentIndexRetryMap& retry_map = index_retry_map_[collection_id];
    for (auto& id : failed_ids) {
        retry_map[id]++;
    }
}

void
DBImpl::IgnoreIndexFailedSegments(snapshot::ID_TYPE collection_id, snapshot::IDS_TYPE& segment_ids) {
    std::lock_guard<std::mutex> lock(index_retry_mutex_);
    SegmentIndexRetryMap& retry_map = index_retry_map_[collection_id];
    snapshot::IDS_TYPE segment_ids_to_build;
    for (auto id : segment_ids) {
        if (retry_map[id] < BUILD_INEDX_RETRY_TIMES) {
            segment_ids_to_build.push_back(id);
        }
    }
    segment_ids.swap(segment_ids_to_build);
}

}  // namespace engine
}  // namespace milvus
