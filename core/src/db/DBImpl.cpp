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

#include <assert.h>
#include <fiu-local.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>

#include "Utils.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "codecs/default/DefaultCodec.h"
#include "db/IDGenerator.h"
#include "db/merge/MergeManagerFactory.h"
#include "engine/EngineFactory.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/BuilderSuspend.h"
#include "index/thirdparty/faiss/utils/distances.h"
#include "insert/MemManagerFactory.h"
#include "meta/MetaConsts.h"
#include "meta/MetaFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "metrics/Metrics.h"
#include "scheduler/Definition.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/job/SearchJob.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"
#include "wal/WalDefinations.h"

namespace milvus {
namespace engine {

namespace {
constexpr uint64_t BACKGROUND_METRIC_INTERVAL = 1;
constexpr uint64_t BACKGROUND_INDEX_INTERVAL = 1;
constexpr uint64_t WAIT_BUILD_INDEX_INTERVAL = 5;

constexpr const char* JSON_ROW_COUNT = "row_count";
constexpr const char* JSON_PARTITIONS = "partitions";
constexpr const char* JSON_PARTITION_TAG = "tag";
constexpr const char* JSON_SEGMENTS = "segments";
constexpr const char* JSON_SEGMENT_NAME = "name";
constexpr const char* JSON_INDEX_NAME = "index_name";
constexpr const char* JSON_DATA_SIZE = "data_size";

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");

}  // namespace

DBImpl::DBImpl(const DBOptions& options)
    : options_(options), initialized_(false), merge_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    meta_ptr_ = MetaFactory::Build(options.meta_, options.mode_);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);
    merge_mgr_ptr_ = MergeManagerFactory::Build(meta_ptr_, options_);

    if (options_.wal_enable_) {
        wal::MXLogConfiguration mxlog_config;
        mxlog_config.recovery_error_ignore = options_.recovery_error_ignore_;
        // 2 buffers in the WAL
        mxlog_config.buffer_size = options_.buffer_size_ / 2;
        mxlog_config.mxlog_path = options_.mxlog_path_;
        wal_mgr_ = std::make_shared<wal::WalManager>(mxlog_config);
    }

    SetIdentity("DBImpl");
    AddCacheInsertDataListener();
    AddUseBlasThresholdListener();

    Start();
}

DBImpl::~DBImpl() {
    Stop();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// external api
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    if (options_.mode_ == DBOptions::MODE::SINGLE || options_.mode_ == DBOptions::MODE::CLUSTER_WRITABLE) {
        // server may be closed unexpected, these un-merge files need to be merged when server restart
        // and soft-delete files need to be deleted when server restart
        // warnning: read-only node is not allow to do merge
        std::set<std::string> merge_collection_ids;
        std::vector<meta::CollectionSchema> collection_schema_array;
        meta_ptr_->AllCollections(collection_schema_array);
        for (auto& schema : collection_schema_array) {
            merge_collection_ids.insert(schema.collection_id_);
        }
        StartMergeTask(merge_collection_ids, true);
    }

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

        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background wal thread
            bg_wal_thread_ = std::thread(&DBImpl::BackgroundWalThread, this);
        }
    } else {
        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background flush thread
            bg_flush_thread_ = std::thread(&DBImpl::BackgroundFlushThread, this);
        }
    }

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background build index thread
        bg_index_thread_ = std::thread(&DBImpl::BackgroundIndexThread, this);
    }

    // background metric thread
    fiu_do_on("options_metric_enable", options_.metric_enable_ = true);
    if (options_.metric_enable_) {
        bg_metric_thread_ = std::thread(&DBImpl::BackgroundMetricThread, this);
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

        meta_ptr_->CleanUpShadowFiles();
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
DBImpl::DropAll() {
    return meta_ptr_->DropAll();
}

Status
DBImpl::CreateCollection(meta::CollectionSchema& collection_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::CollectionSchema temp_schema = collection_schema;
    temp_schema.index_file_size_ *= MB;  // store as MB
    if (options_.wal_enable_) {
        temp_schema.flush_lsn_ = wal_mgr_->GetLastAppliedLsn();
    }

    auto status = meta_ptr_->CreateCollection(temp_schema);
    if (options_.wal_enable_ && status.ok()) {
        wal_mgr_->CreateCollection(collection_schema.collection_id_);
    }

    return status;
}

Status
DBImpl::DropCollection(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

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

    return Status::OK();
}

Status
DBImpl::DescribeCollection(meta::CollectionSchema& collection_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto stat = meta_ptr_->DescribeCollection(collection_schema);
    collection_schema.index_file_size_ /= MB;  // return as MB
    return stat;
}

Status
DBImpl::HasCollection(const std::string& collection_id, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasCollection(collection_id, has_or_not, false);
}

Status
DBImpl::HasNativeCollection(const std::string& collection_id, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasCollection(collection_id, has_or_not, true);
}

Status
DBImpl::AllCollections(std::vector<meta::CollectionSchema>& collection_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::vector<meta::CollectionSchema> all_collections;
    auto status = meta_ptr_->AllCollections(all_collections);

    // only return real collections, dont return partition collections
    collection_schema_array.clear();
    for (auto& schema : all_collections) {
        if (schema.owner_collection_.empty()) {
            collection_schema_array.push_back(schema);
        }
    }

    return status;
}

Status
DBImpl::GetCollectionInfo(const std::string& collection_id, std::string& collection_info) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step1: get all partition ids
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);

    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::INDEX};

    milvus::json json_info;
    milvus::json json_partitions;
    size_t total_row_count = 0;

    auto get_info = [&](const std::string& col_id, const std::string& tag) {
        meta::FilesHolder files_holder;
        status = meta_ptr_->FilesByType(col_id, file_types, files_holder);
        if (!status.ok()) {
            std::string err_msg = "Failed to get collection info: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return Status(DB_ERROR, err_msg);
        }

        milvus::json json_partition;
        json_partition[JSON_PARTITION_TAG] = tag;

        milvus::json json_segments;
        size_t row_count = 0;
        milvus::engine::meta::SegmentsSchema& collection_files = files_holder.HoldFiles();
        for (auto& file : collection_files) {
            // if the file file_id = segment_id, it must be a raw file, the index name is IDMAP
            // else, it is an index file, use engine_type_ to mapping the name
            std::string index_name = utils::RAWDATA_INDEX_NAME;
            if (file.segment_id_ != file.file_id_) {
                index_name = utils::GetIndexName(file.engine_type_);
            }

            milvus::json json_segment;
            json_segment[JSON_SEGMENT_NAME] = file.segment_id_;
            json_segment[JSON_ROW_COUNT] = file.row_count_;
            json_segment[JSON_INDEX_NAME] = index_name;
            json_segment[JSON_DATA_SIZE] = (int64_t)file.file_size_;
            json_segments.push_back(json_segment);

            row_count += file.row_count_;
            total_row_count += file.row_count_;
        }

        json_partition[JSON_ROW_COUNT] = row_count;
        json_partition[JSON_SEGMENTS] = json_segments;

        json_partitions.push_back(json_partition);

        return Status::OK();
    };

    // step2: get default partition info
    status = get_info(collection_id, milvus::engine::DEFAULT_PARTITON_TAG);
    if (!status.ok()) {
        return status;
    }

    // step3: get partitions info
    for (auto& schema : partition_array) {
        status = get_info(schema.collection_id_, schema.partition_tag_);
        if (!status.ok()) {
            return status;
        }
    }

    json_info[JSON_ROW_COUNT] = total_row_count;
    json_info[JSON_PARTITIONS] = json_partitions;

    collection_info = json_info.dump();

    return Status::OK();
}

Status
DBImpl::PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                          const std::vector<std::string>& partition_tags, bool force) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    bool is_all_search_file = true;
    server::Config::GetInstance().GetGeneralConfigSearchRawEnable(is_all_search_file);

    // step 1: get all collection files from collection
    meta::FilesHolder files_holder;
    Status status = CollectFilesToSearch(collection_id, partition_tags, is_all_search_file, files_holder);
    if (!status.ok()) {
        return status;
    }

    if (files_holder.HoldFiles().empty()) {
        LOG_ENGINE_DEBUG_ << "Could not get any file to load";
        return Status::OK();  // no files to search
    }

    int64_t size = 0;
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t available_size = cache_total - cache_usage;

    // step 2: load file one by one
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

        auto json = milvus::json::parse(file.index_params_);
        ExecutionEnginePtr engine = EngineFactory::Build(file.dimension_, file.location_, engine_type,
                                                         (MetricType)file.metric_type_, json, file.updated_time_);
        fiu_do_on("DBImpl.PreloadCollection.null_engine", engine = nullptr);
        if (engine == nullptr) {
            LOG_ENGINE_ERROR_ << "Invalid engine type";
            return Status(DB_ERROR, "Invalid engine type");
        }

        fiu_do_on("DBImpl.PreloadCollection.exceed_cache", size = available_size + 1);

        try {
            fiu_do_on("DBImpl.PreloadCollection.engine_throw_exception", throw std::exception());
            std::string msg = "Pre-loaded file: " + file.file_id_ + " size: " + std::to_string(file.file_size_);
            TimeRecorderAuto rc_1(msg);
            status = engine->Load(false, true);
            if (!status.ok()) {
                return status;
            }

            size += engine->Size();
            if (!force && size > available_size) {
                LOG_ENGINE_DEBUG_ << "Pre-load cancelled since cache is almost full";
                return Status(SERVER_CACHE_FULL, "Cache is full");
            }
        } catch (std::exception& ex) {
            std::string msg = "Pre-load collection encounter exception: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
    }

    return Status::OK();
}

Status
DBImpl::ReleaseCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                          const std::vector<std::string>& partition_tags) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: get all collection files from collection
    meta::FilesHolder files_holder;
    Status status = CollectFilesToSearch(collection_id, partition_tags, true, files_holder);
    if (!status.ok()) {
        return status;
    }

    if (files_holder.HoldFiles().empty()) {
        return Status::OK();  // no files to search
    }

    // step 2: release file one by one
    milvus::engine::meta::SegmentsSchema& files_array = files_holder.HoldFiles();
    TimeRecorderAuto rc("Release collection:" + collection_id);
    for (auto& file : files_array) {
        // client break the connection, no need to continue
        if (context && context->IsConnectionBroken()) {
            LOG_ENGINE_DEBUG_ << "Client connection broken, stop release collection";
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

        auto json = milvus::json::parse(file.index_params_);
        ExecutionEnginePtr engine = EngineFactory::Build(file.dimension_, file.location_, engine_type,
                                                         (MetricType)file.metric_type_, json, file.updated_time_);

        if (engine == nullptr) {
            LOG_ENGINE_ERROR_ << "Invalid engine type";
            continue;
        }

        status = engine->ReleaseCache();
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::ReLoadSegmentsDeletedDocs(const std::string& collection_id, const std::vector<int64_t>& segment_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }
#if 0  // todo
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

        auto data_obj_ptr = cache::CpuCacheMgr::GetInstance()->GetItem(file.location_);
        auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
        if (nullptr == index) {
            LOG_ENGINE_WARNING_ << "Index " << file.location_ << " not found";
            continue;
        }

        segment::SegmentReader segment_reader(segment_dir);

        segment::DeletedDocsPtr delete_docs = std::make_shared<segment::DeletedDocs>();
        segment_reader.LoadDeletedDocs(delete_docs);
        auto& docs_offsets = delete_docs->GetDeletedDocs();
        if (docs_offsets.empty()) {
            LOG_ENGINE_DEBUG_ << "delete_docs is empty";
            continue;
        }

        faiss::ConcurrentBitsetPtr blacklist = index->GetBlacklist();
        if (nullptr == blacklist) {
            LOG_ENGINE_WARNING_ << "Index " << file.location_ << " is empty";
            faiss::ConcurrentBitsetPtr concurrent_bitset_ptr =
                std::make_shared<faiss::ConcurrentBitset>(index->Count());
            index->SetBlacklist(concurrent_bitset_ptr);
            blacklist = concurrent_bitset_ptr;
        }

        for (auto& i : docs_offsets) {
            blacklist->set(i);
        }
    }
#endif
    return Status::OK();
}

Status
DBImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->UpdateCollectionFlag(collection_id, flag);
}

Status
DBImpl::GetCollectionRowCount(const std::string& collection_id, uint64_t& row_count) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return GetCollectionRowCountRecursively(collection_id, row_count);
}

Status
DBImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name,
                        const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    uint64_t lsn = 0;
    if (options_.wal_enable_) {
        lsn = wal_mgr_->GetLastAppliedLsn();
    } else {
        meta_ptr_->GetCollectionFlushLSN(collection_id, lsn);
    }

    auto status = meta_ptr_->CreatePartition(collection_id, partition_name, partition_tag, lsn);
    if (options_.wal_enable_ && status.ok()) {
        wal_mgr_->CreatePartition(collection_id, partition_tag);
    }
    return status;
}

Status
DBImpl::HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // trim side-blank of tag, only compare valid characters
    // for example: " ab cd " is treated as "ab cd"
    std::string valid_tag = tag;
    server::StringHelpFunctions::TrimStringBlank(valid_tag);

    if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
        has_or_not = true;
        return Status::OK();
    }

    return meta_ptr_->HasPartition(collection_id, valid_tag, has_or_not);
}

Status
DBImpl::DropPartition(const std::string& partition_name) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    mem_mgr_->EraseMemVector(partition_name);                // not allow insert
    auto status = meta_ptr_->DropPartition(partition_name);  // soft delete collection
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    return Status::OK();
}

Status
DBImpl::DropPartitionByTag(const std::string& collection_id, const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::string partition_name;
    auto status = meta_ptr_->GetPartitionName(collection_id, partition_tag, partition_name);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    if (options_.wal_enable_) {
        wal_mgr_->DropPartition(collection_id, partition_tag);
    }

    return DropPartition(partition_name);
}

Status
DBImpl::ShowPartitions(const std::string& collection_id, std::vector<meta::CollectionSchema>& partition_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->ShowPartitions(collection_id, partition_schema_array);
}

Status
DBImpl::CountPartitions(const std::string& collection_id, int64_t& partition_count) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->CountPartitions(collection_id, partition_count);
}

Status
DBImpl::InsertVectors(const std::string& collection_id, const std::string& partition_tag, VectorsData& vectors) {
    //    LOG_ENGINE_DEBUG_ << "Insert " << n << " vectors to cache";
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // insert vectors into target collection
    // (zhiru): generate ids
    if (vectors.id_array_.empty()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        Status status = id_generator.GetNextIDNumbers(vectors.vector_count_, vectors.id_array_);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Get next id number fail: %s", "insert", 0, status.message().c_str());
            return status;
        }
    }

    Status status;
    if (options_.wal_enable_) {
        std::string target_collection_name;
        status = GetPartitionByTag(collection_id, partition_tag, target_collection_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Get partition fail: %s", "insert", 0, status.message().c_str());
            return status;
        }

        if (!vectors.float_data_.empty()) {
            wal_mgr_->Insert(collection_id, partition_tag, vectors.id_array_, vectors.float_data_);
        } else if (!vectors.binary_data_.empty()) {
            wal_mgr_->Insert(collection_id, partition_tag, vectors.id_array_, vectors.binary_data_);
        }
        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.collection_id = collection_id;
        record.partition_tag = partition_tag;
        record.ids = vectors.id_array_.data();
        record.length = vectors.vector_count_;
        if (vectors.binary_data_.empty()) {
            record.type = wal::MXLogType::InsertVector;
            record.data = vectors.float_data_.data();
            record.data_size = vectors.float_data_.size() * sizeof(float);
        } else {
            record.type = wal::MXLogType::InsertBinary;
            record.ids = vectors.id_array_.data();
            record.length = vectors.vector_count_;
            record.data = vectors.binary_data_.data();
            record.data_size = vectors.binary_data_.size() * sizeof(uint8_t);
        }

        status = ExecWalRecord(record);
    }

    return status;
}

Status
CopyToAttr(std::vector<uint8_t>& record, uint64_t row_num, const std::vector<std::string>& field_names,
           std::unordered_map<std::string, meta::hybrid::DataType>& attr_types,
           std::unordered_map<std::string, std::vector<uint8_t>>& attr_datas,
           std::unordered_map<std::string, uint64_t>& attr_nbytes,
           std::unordered_map<std::string, uint64_t>& attr_data_size) {
    uint64_t offset = 0;
    for (auto name : field_names) {
        switch (attr_types.at(name)) {
            case meta::hybrid::DataType::INT8: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int8_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int8_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int8_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int8_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int8_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT16: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int16_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int16_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int16_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int16_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int16_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT32: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int32_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int32_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int32_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int32_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int32_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT64: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int64_t));
                memcpy(data.data(), record.data() + offset, row_num * sizeof(int64_t));
                attr_datas.insert(std::make_pair(name, data));

                std::vector<int64_t> test_data(row_num);
                memcpy(test_data.data(), record.data(), row_num * sizeof(int64_t));

                attr_nbytes.insert(std::make_pair(name, sizeof(int64_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int64_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::FLOAT: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(float));

                std::vector<double> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(double));

                std::vector<float> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(float));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(float)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(float)));
                offset += row_num * sizeof(double);
                break;
            }
            case meta::hybrid::DataType::DOUBLE: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(double));
                memcpy(data.data(), record.data() + offset, row_num * sizeof(double));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(double)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(double)));
                offset += row_num * sizeof(double);
                break;
            }
            default:
                break;
        }
    }
    return Status::OK();
}

Status
DBImpl::DeleteVectors(const std::string& collection_id, const std::string& partition_tag, IDNumbers vector_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DeleteById(collection_id, partition_tag, vector_ids);
        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.type = wal::MXLogType::Delete;
        record.collection_id = collection_id;
        record.partition_tag = partition_tag;
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
DBImpl::GetVectorsByID(const engine::meta::CollectionSchema& collection, const std::string& partition_tag,
                       const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    if (id_array.empty()) {
        LOG_ENGINE_DEBUG_ << "No id specified to get vector by id";
        return Status(DB_ERROR, "No id specified");
    }

    meta::FilesHolder files_holder;
    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};

    if (partition_tag.empty()) {
        std::vector<meta::CollectionSchema> collection_array;
        auto status = meta_ptr_->ShowPartitions(collection.collection_id_, collection_array);

        collection_array.push_back(collection);
        status = meta_ptr_->FilesByTypeEx(collection_array, file_types, files_holder);
        if (!status.ok()) {
            std::string err_msg = "Failed to get files for GetVectorByID: " + status.message();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    } else {
        std::string target_collection_name;
        auto status = GetPartitionByTag(collection.collection_id_, partition_tag, target_collection_name);
        if (!status.ok()) {
            return status;  // didn't match any partition.
        }
        status = meta_ptr_->FilesByType(target_collection_name, file_types, files_holder);
    }

    if (files_holder.HoldFiles().empty()) {
        LOG_ENGINE_DEBUG_ << "No files to get vector by id from";
        return Status(DB_NOT_FOUND, "Collection or partition is empty");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();
    auto status = GetVectorsByIdHelper(id_array, vectors, files_holder);
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

    bool uids_from_cache;
    segment::UidsPtr uids_ptr;
    {
        auto index = cache::CpuCacheMgr::GetInstance()->GetItem(collection_files[0].location_);
        if (index != nullptr) {
            uids_ptr = std::static_pointer_cast<knowhere::VecIndex>(index)->GetUids();
            uids_from_cache = true;
        } else {
            status = segment_reader.LoadUids(uids_ptr);
            if (!status.ok()) {
                return status;
            }
            uids_from_cache = false;
        }
    }

    segment::DeletedDocsPtr deleted_docs_ptr;
    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
    if (!status.ok()) {
        return status;
    }
    auto& deleted_offset = deleted_docs_ptr->GetMutableDeletedDocs();

    // step 4: construct id array
    if (deleted_offset.empty()) {
        if (!uids_from_cache) {
            vector_ids.swap(*uids_ptr);
        } else {
            vector_ids = *uids_ptr;
        }
    } else {
        std::sort(deleted_offset.begin(), deleted_offset.end());

        vector_ids.clear();
        vector_ids.reserve(uids_ptr->size());

        auto id_begin_iter = uids_ptr->begin();
        auto id_end_iter = uids_ptr->end();
        int offset = 0;
        for (size_t i = 0; i < deleted_offset.size(); i++) {
            if (offset < deleted_offset[i]) {
                vector_ids.insert(vector_ids.end(), id_begin_iter + offset, id_begin_iter + deleted_offset[i]);
            }
            offset = deleted_offset[i] + 1;
        }
        if (offset < uids_ptr->size()) {
            vector_ids.insert(vector_ids.end(), id_begin_iter + offset, id_end_iter);
        }
    }

    return status;
}

Status
DBImpl::GetVectorsByIdHelper(const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors,
                             meta::FilesHolder& files_holder) {
    // attention: this is a copy, not a reference, since the files_holder.UnMarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    std::string msg = "Getting vector by id in " + std::to_string(files.size()) +
                      " files, id count = " + std::to_string(id_array.size());
    TimeRecorderAuto rc(msg);

    bool is_binary = false;
    size_t single_vector_bytes = 0;
    if (!files.empty()) {
        auto& file = files[0];
        is_binary = utils::IsBinaryMetricType(file.metric_type_);
        single_vector_bytes = is_binary ? (file.dimension_ / 8) : (file.dimension_ * sizeof(float));
    }

    // sometimes not all of id_array can be found, we need to return empty vector for id not found
    // for example:
    // id_array = [1, -1, 2, -1, 3]
    // vectors should return [valid_vector, empty_vector, valid_vector, empty_vector, valid_vector]
    // the temp_ids is to ensure returned vector sequence is consist with id_array
    struct IdInfo {
        int64_t sequence = -1;
        IDNumber vec_id = 0;
        bool possible_in = false;
        int64_t offset = -1;
    };

    std::vector<IdInfo> temp_ids;
    temp_ids.resize(id_array.size());
    for (size_t i = 0; i < id_array.size(); i++) {
        temp_ids[i].sequence = i;
        temp_ids[i].vec_id = id_array[i];
        temp_ids[i].possible_in = false;
        temp_ids[i].offset = -1;
    }
    vectors.resize(id_array.size());

    // Iterate each segment to find vectors
    for (auto& file : files) {
        if (temp_ids.empty()) {
            break;  // all vectors found, no need to continue
        }

        // SegmentReader
        std::string segment_dir;
        engine::utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);

        // Method to read uid file, fetch from cache firstly
        segment::UidsPtr uids_ptr = nullptr;
        auto LoadUid = [&]() {
            auto index = cache::CpuCacheMgr::GetInstance()->GetItem(file.location_);
            if (index != nullptr) {
                uids_ptr = std::static_pointer_cast<knowhere::VecIndex>(index)->GetUids();
                return Status::OK();
            }

            return segment_reader.LoadUids(uids_ptr);
        };

        // Method to read delete-docs file
        segment::DeletedDocsPtr deleted_docs_ptr = nullptr;
        auto LoadDeleteDoc = [&]() { return segment_reader.LoadDeletedDocs(deleted_docs_ptr); };

        // Read bloom filter file, if the file doesn't exist, regenerate it
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        auto status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr, false);
        fiu_do_on("DBImpl.GetVectorsByIdHelper.FailedToLoadBloomFilter",
                  (status = Status(DB_ERROR, ""), id_bloom_filter_ptr = nullptr));
        if (!status.ok()) {
            // Some accidents may cause the bloom filter file destroyed.
            // If failed to load bloom filter, just to create a new one.
            if (!(status = LoadUid()).ok()) {
                return status;
            }
            if (!(status = LoadDeleteDoc()).ok()) {
                return status;
            }

            codec::DefaultCodec default_codec;
            default_codec.GetIdBloomFilterFormat()->create(uids_ptr->size(), id_bloom_filter_ptr);
            id_bloom_filter_ptr->Add(*uids_ptr, deleted_docs_ptr->GetMutableDeletedDocs());
            LOG_ENGINE_DEBUG_ << "A new bloom filter is created";

            segment::SegmentWriter segment_writer(segment_dir);
            segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
        }

        // Check if the id is present in bloom filter.
        bool possible_in = false;
        for (size_t i = 0; i < temp_ids.size(); ++i) {
            // Check if the id is present in bloom filter.
            if (id_bloom_filter_ptr->Check(temp_ids[i].vec_id)) {
                temp_ids[i].possible_in = true;
                possible_in = true;
            }
        }

        // No any id in this segment, go to next segment
        if (!possible_in) {
            continue;
        }

        // Since some ids in this segment, we need to read uids file and deleted-docs file
        if (!(status = LoadUid()).ok()) {
            return status;
        }
        LoadDeleteDoc();

        // Method use uids and deleted-docs to check whether id is in segment
        auto FindId = [&](int64_t i) {
            if (!temp_ids[i].possible_in) {
                return;
            }
            temp_ids[i].possible_in = false;

            // If the id is in uids and not in deleted-docs, that means this id is found
            auto found = std::find(uids_ptr->begin(), uids_ptr->end(), temp_ids[i].vec_id);
            if (found == uids_ptr->end()) {
                return;
            }

            auto offset = std::distance(uids_ptr->begin(), found);
            if (deleted_docs_ptr) {
                auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
                auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
                if (deleted != deleted_docs.end()) {
                    return;
                }
            }

            temp_ids[i].offset = offset;
        };

        // For large number id array, use multi-threads to find, otherwise single thread.
        // In our test, when id count is 4, single thread performance is almost equal to multi threads.
        // So we use a hard code value "4" here as boundary of single thread and multi threads.
#pragma omp parallel for if (temp_ids.size() > 4)
        for (size_t i = 0; i < temp_ids.size(); ++i) {
            FindId(i);
        }

        // Fetch vector data
        for (size_t i = 0; i < temp_ids.size();) {
            auto& iter = temp_ids[i];
            if (iter.offset >= 0) {
                // each id must has a VectorsData
                // if vector not found for an id, its VectorsData's vector_count = 0, else 1
                VectorsData& vector_ref = vectors[iter.sequence];

                // Load raw vector
                std::vector<uint8_t> raw_vector;
                status = segment_reader.LoadsSingleVector(iter.offset * single_vector_bytes, single_vector_bytes,
                                                          raw_vector);
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

                // After this retrieving vector data for this id, copy the tail id to this position.
                // We didn't use std::vector::erase() because erase() could do copy many times.
                iter = temp_ids.back();
                temp_ids.resize(temp_ids.size() - 1);
            } else {
                i++;
            }
        }

        // unmark file, allow the file to be deleted
        files_holder.UnmarkFile(file);
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

    // step 2: get old index
    CollectionIndex old_index;
    status = DescribeIndex(collection_id, old_index);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to get collection index info for collection: " << collection_id;
        return status;
    }

    // fix issue #4838 create a new index need waiting a long time when other index is creating
    // if the collection is empty, set its index and return
    uint64_t row_count = 0;
    status = GetCollectionRowCountRecursively(collection_id, row_count);
    if (status.ok() && row_count == 0) {
        CollectionIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateCollection
        return UpdateCollectionIndexRecursively(collection_id, new_index, true);
    }

    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);

        // step 3: update index info
        CollectionIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateCollection
        if (!utils::IsSameIndex(old_index, new_index)) {
            status = UpdateCollectionIndexRecursively(collection_id, new_index, false);
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
    status = GetVectorsByID(collection_schema, "", id_array, vectors);
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
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
              const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
              const VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    bool is_all_search_file = true;
    server::Config::GetInstance().GetGeneralConfigSearchRawEnable(is_all_search_file);
    // step 1: get all collection files from collection
    meta::FilesHolder files_holder;
    Status status = CollectFilesToSearch(collection_id, partition_tags, is_all_search_file, files_holder);
    if (!status.ok()) {
        return status;
    }

    if (files_holder.HoldFiles().empty()) {
        return Status::OK();  // no files to search
    }

    // step 2: do query
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
        // no need to process shadow files
        if (file.file_type_ == milvus::engine::meta::SegmentSchema::FILE_TYPE::NEW ||
            file.file_type_ == milvus::engine::meta::SegmentSchema::FILE_TYPE::NEW_MERGE ||
            file.file_type_ == milvus::engine::meta::SegmentSchema::FILE_TYPE::NEW_INDEX) {
            continue;
        }

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

    Status job_status;
    job->GetStatus(job_status);
    if (!job_status.ok()) {
        return job_status;
    }

    // step 3: construct results
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

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
            Status job_status;
            job->GetStatus(job_status);
            if (!job_status.ok()) {
                LOG_ENGINE_ERROR_ << "Building index job " << job->id() << " failed: " << job_status.ToString();

                index_failed_checker_.MarkFailedIndexFile(file_schema, job_status.message());
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
                            std::set<std::string>& partition_name_array,
                            std::vector<meta::CollectionSchema>& partition_array) {
    std::vector<meta::CollectionSchema> all_partitions;
    auto status = meta_ptr_->ShowPartitions(collection_id, all_partitions);

    for (auto& tag : partition_tags) {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name_array.insert(collection_id);
            continue;
        }

        for (auto& schema : all_partitions) {
            if (server::StringHelpFunctions::IsRegexMatch(schema.partition_tag_, valid_tag)) {
                if (partition_name_array.find(schema.collection_id_) == partition_name_array.end()) {
                    partition_name_array.insert(schema.collection_id_);
                    partition_array.push_back(schema);
                }
            }
        }
    }

    if (partition_name_array.empty()) {
        return Status(DB_PARTITION_NOT_FOUND, "The specified partition does not exist");
    }

    return Status::OK();
}

Status
DBImpl::UpdateCollectionIndexRecursively(const std::string& collection_id, const CollectionIndex& index,
                                         bool meta_only) {
    if (!meta_only) {
        DropIndex(collection_id);
        WaitMergeFileFinish();  // DropIndex called StartMergeTask, need to wait merge thread finish
    }

    auto status = meta_ptr_->UpdateCollectionIndex(collection_id, index);
    fiu_do_on("DBImpl.UpdateCollectionIndexRecursively.fail_update_collection_index",
              status = Status(DB_META_TRANSACTION_FAILED, ""));
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to update collection index info for collection: " << collection_id;
        return status;
    }

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    if (!status.ok()) {
        return status;
    }
    for (auto& schema : partition_array) {
        status = UpdateCollectionIndexRecursively(schema.collection_id_, index, meta_only);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
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
            // if the background index thread finished, it will trigger the index_req_swn_, the ret = no_timeout
            // so, ret = no_timeout means we can check to_index files at once, no need to wait 5 seconds
            repeat++;
            if (repeat % WAIT_BUILD_INDEX_INTERVAL == 0 || ret == std::cv_status::no_timeout) {
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
    }

    LOG_ENGINE_DEBUG_ << "WaitCollectionIndexRecursively finished";

    return Status::OK();
}

Status
DBImpl::DropCollectionIndexRecursively(const std::string& collection_id) {
    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_id;
    index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);
    auto status = meta_ptr_->DropCollectionIndex(collection_id);
    if (!status.ok()) {
        return status;
    }

    // drop partition index
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropCollectionIndexRecursively(schema.collection_id_);
        fiu_do_on("DBImpl.DropCollectionIndexRecursively.fail_drop_collection_Index_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::GetCollectionRowCountRecursively(const std::string& collection_id, uint64_t& row_count) {
    row_count = 0;
    auto status = meta_ptr_->Count(collection_id, row_count);
    if (!status.ok()) {
        return status;
    }

    // get partition row count
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        uint64_t partition_row_count = 0;
        status = meta_ptr_->Count(schema.collection_id_, partition_row_count);
        fiu_do_on("DBImpl.GetCollectionRowCountRecursively.fail_get_collection_rowcount_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        row_count += partition_row_count;
    }

    return Status::OK();
}

Status
DBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
    fiu_return_on("DBImpl.ExexWalRecord.return", Status(););

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
        case wal::MXLogType::InsertBinary: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(uint8_t)),
                                             (const u_int8_t*)record.data, record.lsn);
            force_flush_if_mem_full();

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::InsertVector: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(float)),
                                             (const float*)record.data, record.lsn);
            force_flush_if_mem_full();

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::Delete: {
            // If no partition tag specified, will delete from all partitions under this collection
            // including the collection itself. Else only delete from the specified partiion.
            // If the specified partition is not found, return error.
            std::vector<std::string> collection_ids;
            if (record.partition_tag.empty()) {
                std::vector<meta::CollectionSchema> partition_array;
                status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
                if (!status.ok()) {
                    return status;
                }

                collection_ids.push_back(record.collection_id);
                for (auto& partition : partition_array) {
                    auto& partition_collection_id = partition.collection_id_;
                    collection_ids.emplace_back(partition_collection_id);
                }
            } else {
                std::string target_collection_name;
                status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
                if (!status.ok()) {
                    LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "delete", 0) << "Get partition fail: " << status.message();
                    return status;
                }
                collection_ids.push_back(target_collection_name);
            }

            if (record.length == 1) {
                for (auto& collection_id : collection_ids) {
                    status = mem_mgr_->DeleteVector(collection_id, *record.ids, record.lsn);
                    if (!status.ok()) {
                        return status;
                    }
                }
            } else {
                for (auto& collection_id : collection_ids) {
                    status = mem_mgr_->DeleteVectors(collection_id, record.length, record.ids, record.lsn);
                    if (!status.ok()) {
                        return status;
                    }
                }
            }
            break;
        }

        case wal::MXLogType::Flush: {
            if (!record.collection_id.empty()) {
                // flush one collection
                std::vector<meta::CollectionSchema> partition_array;
                status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
                if (!status.ok()) {
                    return status;
                }

                std::vector<std::string> collection_ids{record.collection_id};
                for (auto& partition : partition_array) {
                    auto& partition_collection_id = partition.collection_id_;
                    collection_ids.emplace_back(partition_collection_id);
                }

                std::set<std::string> flushed_collections;
                for (auto& collection_id : collection_ids) {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(collection_id);
                    if (!status.ok()) {
                        break;
                    }
                    flushed_collections.insert(collection_id);
                }

                collections_flushed(record.collection_id, flushed_collections);

            } else {
                // flush all collections
                std::set<std::string> collection_ids;
                {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(collection_ids);
                }

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

    return status;
}

void
DBImpl::InternalFlush(const std::string& collection_id) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_id;
    ExecWalRecord(record);
}

void
DBImpl::BackgroundWalThread() {
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
                WaitMergeFileFinish();
                WaitBuildIndexFinish();
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
DBImpl::BackgroundFlushThread() {
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
DBImpl::BackgroundMetricThread() {
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
DBImpl::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

void
DBImpl::OnUseBlasThresholdChanged(int64_t threshold) {
    faiss::distance_compute_blas_threshold = threshold;
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

Status
DBImpl::CollectFilesToSearch(const std::string& collection_id, const std::vector<std::string>& partition_tags,
                             bool is_all_search_file, meta::FilesHolder& files_holder) {
    Status status;
    std::set<std::string> partition_ids;
    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole collection
        // get files from root collection
        status = meta_ptr_->FilesToSearch(collection_id, files_holder, is_all_search_file);
        if (!status.ok()) {
            return status;
        }

        // count all partitions
        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        for (auto& schema : partition_array) {
            partition_ids.insert(schema.collection_id_);
        }
    } else {
        // get specified partitions
        std::set<std::string> partition_name_array;
        std::vector<meta::CollectionSchema> partition_array;
        status = GetPartitionsByTags(collection_id, partition_tags, partition_name_array, partition_array);
        if (!status.ok()) {
            return status;  // didn't match any partition.
        }

        for (auto& partition_name : partition_name_array) {
            partition_ids.insert(partition_name);
        }
    }

    // get files from partitions
    status = meta_ptr_->FilesToSearchEx(collection_id, partition_ids, files_holder, is_all_search_file);
    if (!status.ok()) {
        return status;
    }

    return status;
}

}  // namespace engine
}  // namespace milvus
