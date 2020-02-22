// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "db/DBImpl.h"

#include <assert.h>
#include <fiu-local.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <set>
#include <thread>
#include <utility>

#include "Utils.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "db/IDGenerator.h"
#include "engine/EngineFactory.h"
#include "insert/MemMenagerFactory.h"
#include "meta/MetaConsts.h"
#include "meta/MetaFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "metrics/Metrics.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/job/DeleteJob.h"
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

constexpr uint64_t METRIC_ACTION_INTERVAL = 1;
constexpr uint64_t COMPACT_ACTION_INTERVAL = 1;
constexpr uint64_t INDEX_ACTION_INTERVAL = 1;

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");

}  // namespace

DBImpl::DBImpl(const DBOptions& options)
    : options_(options), initialized_(false), compact_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    meta_ptr_ = MetaFactory::Build(options.meta_, options.mode_);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);

    if (options_.wal_enable_) {
        wal::MXLogConfiguration mxlog_config;
        mxlog_config.record_size = options_.record_size_;
        mxlog_config.recovery_error_ignore = options_.recovery_error_ignore_;
        mxlog_config.buffer_size = options_.buffer_size_;
        mxlog_config.mxlog_path = options_.mxlog_path_;
        wal_mgr_ = std::make_shared<wal::WalManager>(mxlog_config);
    }

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

    // ENGINE_LOG_TRACE << "DB service start";
    initialized_.store(true, std::memory_order_release);

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
            // background thread
            bg_wal_thread_ = std::thread(&DBImpl::BackgroundWalTask, this);
        }

    } else {
        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // ENGINE_LOG_TRACE << "StartTimerTasks";
            bg_timer_thread_ = std::thread(&DBImpl::BackgroundTimerTask, this);
        }
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
            // wait flush compaction/buildindex finish
            wal_task_swn_.Notify();
            bg_wal_thread_.join();

        } else {
            // flush all
            wal::MXLogRecord record;
            record.type = wal::MXLogType::Flush;
            ExecWalRecord(record);

            // wait compaction/buildindex finish
            bg_timer_thread_.join();
        }

        meta_ptr_->CleanUpShadowFiles();
    }

    // ENGINE_LOG_TRACE << "DB service stop";
    return Status::OK();
}

Status
DBImpl::DropAll() {
    return meta_ptr_->DropAll();
}

Status
DBImpl::CreateTable(meta::TableSchema& table_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::TableSchema temp_schema = table_schema;
    temp_schema.index_file_size_ *= ONE_MB;  // store as MB
    if (options_.wal_enable_) {
        temp_schema.flush_lsn_ = wal_mgr_->CreateTable(table_schema.table_id_);
    }

    return meta_ptr_->CreateTable(temp_schema);
}

Status
DBImpl::DropTable(const std::string& table_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    if (options_.wal_enable_) {
        wal_mgr_->DropTable(table_id);
    }

    return DropTableRecursively(table_id);
}

Status
DBImpl::DescribeTable(meta::TableSchema& table_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto stat = meta_ptr_->DescribeTable(table_schema);
    table_schema.index_file_size_ /= ONE_MB;  // return as MB
    return stat;
}

Status
DBImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasTable(table_id, has_or_not);
}

Status
DBImpl::AllTables(std::vector<meta::TableSchema>& table_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::vector<meta::TableSchema> all_tables;
    auto status = meta_ptr_->AllTables(all_tables);

    // only return real tables, dont return partition tables
    table_schema_array.clear();
    for (auto& schema : all_tables) {
        if (schema.owner_table_.empty()) {
            table_schema_array.push_back(schema);
        }
    }

    return status;
}

Status
DBImpl::GetTableInfo(const std::string& table_id, TableInfo& table_info) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step1: get all partition ids
    std::vector<std::pair<std::string, std::string>> name2tag = {{table_id, milvus::engine::DEFAULT_PARTITON_TAG}};
    std::vector<meta::TableSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        name2tag.push_back(std::make_pair(schema.table_id_, schema.partition_tag_));
    }

    // step2: get native table info
    std::vector<int> file_types{meta::TableFileSchema::FILE_TYPE::RAW, meta::TableFileSchema::FILE_TYPE::TO_INDEX,
                                meta::TableFileSchema::FILE_TYPE::INDEX};

    static std::map<int32_t, std::string> index_type_name = {
        {(int32_t)engine::EngineType::FAISS_IDMAP, "IDMAP"},
        {(int32_t)engine::EngineType::FAISS_IVFFLAT, "IVFFLAT"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8, "IVFSQ8"},
        {(int32_t)engine::EngineType::NSG_MIX, "NSG"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8H, "IVFSQ8H"},
        {(int32_t)engine::EngineType::FAISS_PQ, "PQ"},
        {(int32_t)engine::EngineType::SPTAG_KDT, "KDT"},
        {(int32_t)engine::EngineType::SPTAG_BKT, "BKT"},
        {(int32_t)engine::EngineType::FAISS_BIN_IDMAP, "IDMAP"},
        {(int32_t)engine::EngineType::FAISS_BIN_IVFFLAT, "IVFFLAT"},
    };

    for (auto& name_tag : name2tag) {
        meta::TableFilesSchema table_files;
        status = meta_ptr_->FilesByType(name_tag.first, file_types, table_files);
        if (!status.ok()) {
            std::string err_msg = "Failed to get table info: " + status.ToString();
            ENGINE_LOG_ERROR << err_msg;
            return Status(DB_ERROR, err_msg);
        }

        std::vector<SegmentStat> segments_stat;
        for (auto& file : table_files) {
            SegmentStat seg_stat;
            seg_stat.name_ = file.segment_id_;
            seg_stat.row_count_ = (int64_t)file.row_count_;
            seg_stat.index_name_ = index_type_name[file.engine_type_];
            seg_stat.data_size_ = (int64_t)file.file_size_;
            segments_stat.emplace_back(seg_stat);
        }

        PartitionStat partition_stat;
        if (name_tag.first == table_id) {
            partition_stat.tag_ = milvus::engine::DEFAULT_PARTITON_TAG;
        } else {
            partition_stat.tag_ = name_tag.second;
        }

        partition_stat.segments_stat_.swap(segments_stat);
        table_info.partitions_stat_.emplace_back(partition_stat);
    }

    return Status::OK();
}

Status
DBImpl::PreloadTable(const std::string& table_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: get all table files from parent table
    std::vector<size_t> ids;
    meta::TableFilesSchema files_array;
    auto status = GetFilesToSearch(table_id, ids, files_array);
    if (!status.ok()) {
        return status;
    }

    // step 2: get files from partition tables
    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = GetFilesToSearch(schema.table_id_, ids, files_array);
    }

    int64_t size = 0;
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t available_size = cache_total - cache_usage;

    // step 3: load file one by one
    ENGINE_LOG_DEBUG << "Begin pre-load table:" + table_id + ", totally " << files_array.size()
                     << " files need to be pre-loaded";
    TimeRecorderAuto rc("Pre-load table:" + table_id);
    for (auto& file : files_array) {
        EngineType engine_type;
        if (file.file_type_ == meta::TableFileSchema::FILE_TYPE::RAW ||
            file.file_type_ == meta::TableFileSchema::FILE_TYPE::TO_INDEX ||
            file.file_type_ == meta::TableFileSchema::FILE_TYPE::BACKUP) {
            engine_type = server::ValidationUtil::IsBinaryMetricType(file.metric_type_) ? EngineType::FAISS_BIN_IDMAP
                                                                                        : EngineType::FAISS_IDMAP;
        } else {
            engine_type = (EngineType)file.engine_type_;
        }
        ExecutionEnginePtr engine = EngineFactory::Build(file.dimension_, file.location_, engine_type,
                                                         (MetricType)file.metric_type_, file.nlist_);
        if (engine == nullptr) {
            ENGINE_LOG_ERROR << "Invalid engine type";
            return Status(DB_ERROR, "Invalid engine type");
        }

        size += engine->PhysicalSize();
        if (size > available_size) {
            ENGINE_LOG_DEBUG << "Pre-load canceled since cache almost full";
            return Status(SERVER_CACHE_FULL, "Cache is full");
        } else {
            try {
                std::string msg = "Pre-loaded file: " + file.file_id_ + " size: " + std::to_string(file.file_size_);
                TimeRecorderAuto rc_1(msg);
                engine->Load(true);
            } catch (std::exception& ex) {
                std::string msg = "Pre-load table encounter exception: " + std::string(ex.what());
                ENGINE_LOG_ERROR << msg;
                return Status(DB_ERROR, msg);
            }
        }
    }

    return Status::OK();
}

Status
DBImpl::UpdateTableFlag(const std::string& table_id, int64_t flag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->UpdateTableFlag(table_id, flag);
}

Status
DBImpl::GetTableRowCount(const std::string& table_id, uint64_t& row_count) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return GetTableRowCountRecursively(table_id, row_count);
}

Status
DBImpl::CreatePartition(const std::string& table_id, const std::string& partition_name,
                        const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    uint64_t lsn = 0;
    meta_ptr_->GetTableFlushLSN(table_id, lsn);
    return meta_ptr_->CreatePartition(table_id, partition_name, partition_tag, lsn);
}

Status
DBImpl::DropPartition(const std::string& partition_name) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto status = mem_mgr_->EraseMemVector(partition_name);  // not allow insert
    status = meta_ptr_->DropPartition(partition_name);       // soft delete table

    // scheduler will determine when to delete table files
    auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
    scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(partition_name, meta_ptr_, nres);
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitAndDelete();

    return Status::OK();
}

Status
DBImpl::DropPartitionByTag(const std::string& table_id, const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::string partition_name;
    auto status = meta_ptr_->GetPartitionName(table_id, partition_tag, partition_name);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << status.message();
        return status;
    }

    return DropPartition(partition_name);
}

Status
DBImpl::ShowPartitions(const std::string& table_id, std::vector<meta::TableSchema>& partition_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->ShowPartitions(table_id, partition_schema_array);
}

Status
DBImpl::InsertVectors(const std::string& table_id, const std::string& partition_tag, VectorsData& vectors) {
    //    ENGINE_LOG_DEBUG << "Insert " << n << " vectors to cache";
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // insert vectors into target table
    // (zhiru): generate ids
    if (vectors.id_array_.empty()) {
        auto id_generator = std::make_shared<SimpleIDGenerator>();
        id_generator->GetNextIDNumbers(vectors.vector_count_, vectors.id_array_);
    }

    Status status;
    if (options_.wal_enable_) {
        std::string target_table_name;
        status = GetPartitionByTag(table_id, partition_tag, target_table_name);
        if (!status.ok()) {
            return status;
        }

        if (!vectors.float_data_.empty()) {
            wal_mgr_->Insert(table_id, partition_tag, vectors.id_array_, vectors.float_data_);
        } else if (!vectors.binary_data_.empty()) {
            wal_mgr_->Insert(table_id, partition_tag, vectors.id_array_, vectors.binary_data_);
        }
        wal_task_swn_.Notify();

    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.table_id = table_id;
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
DBImpl::DeleteVector(const std::string& table_id, IDNumber vector_id) {
    IDNumbers ids;
    ids.push_back(vector_id);
    return DeleteVectors(table_id, ids);
}

Status
DBImpl::DeleteVectors(const std::string& table_id, IDNumbers vector_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DeleteById(table_id, vector_ids);
        wal_task_swn_.Notify();

    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.type = wal::MXLogType::Delete;
        record.table_id = table_id;
        record.ids = vector_ids.data();
        record.length = vector_ids.size();

        status = ExecWalRecord(record);
    }

    return status;
}

Status
DBImpl::Flush(const std::string& table_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    ENGINE_LOG_DEBUG << "Flushing table: " << table_id;

    Status status;
    if (options_.wal_enable_) {
        auto lsn = wal_mgr_->Flush(table_id);
        if (lsn != 0) {
            wal_task_swn_.Notify();
            flush_task_swn_.Wait();
        }

    } else {
        wal::MXLogRecord record;
        record.type = wal::MXLogType::Flush;
        record.table_id = table_id;
        status = ExecWalRecord(record);
    }

    return status;
}

Status
DBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // ENGINE_LOG_DEBUG << "Flushing all tables";

    Status status;
    if (options_.wal_enable_) {
        auto lsn = wal_mgr_->Flush();
        if (lsn != 0) {
            wal_task_swn_.Notify();
            flush_task_swn_.Wait();
        }
    } else {
        wal::MXLogRecord record;
        record.type = wal::MXLogType::Flush;
        status = ExecWalRecord(record);
    }

    return status;
}

Status
DBImpl::Compact(const std::string& table_id) {
    // TODO: WAL???
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    bool has_table;
    auto status = HasTable(table_id, has_table);
    if (!has_table) {
        ENGINE_LOG_ERROR << "Table to compact does not exist: " << table_id;
        return Status(DB_NOT_FOUND, "Table to compact does not exist");
    }
    if (!status.ok()) {
        return Status(DB_ERROR, status.message());
    }

    ENGINE_LOG_DEBUG << "Compacting table: " << table_id;

    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

    // Drop all index
    status = DropIndex(table_id);
    if (!status.ok()) {
        std::string err_msg = "Failed to drop index in compact: " + status.message();
        ENGINE_LOG_ERROR << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    // Get files to compact from meta.
    std::vector<int> file_types{meta::TableFileSchema::FILE_TYPE::RAW, meta::TableFileSchema::FILE_TYPE::TO_INDEX};
    meta::TableFilesSchema files_to_compact;
    status = meta_ptr_->FilesByType(table_id, file_types, files_to_compact);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files to compact: " + status.message();
        ENGINE_LOG_ERROR << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    ENGINE_LOG_DEBUG << "Found " << files_to_compact.size() << " segment to compact";

    ongoing_files_checker_.MarkOngoingFiles(files_to_compact);
    for (auto& file : files_to_compact) {
        status = CompactFile(table_id, file);
    }
    ongoing_files_checker_.UnmarkOngoingFiles(files_to_compact);

    ENGINE_LOG_DEBUG << "Finished compacting table: " << table_id;

    return status;
}

Status
DBImpl::CompactFile(const std::string& table_id, const milvus::engine::meta::TableFileSchema& file) {
    ENGINE_LOG_DEBUG << "Compacting segment " << file.segment_id_ << " for table: " << table_id;

    // Create new table file
    meta::TableFileSchema compacted_file;
    compacted_file.table_id_ = table_id;
    // compacted_file.date_ = date;
    compacted_file.file_type_ = meta::TableFileSchema::NEW_MERGE;  // TODO: use NEW_MERGE for now
    Status status = meta_ptr_->CreateTableFile(compacted_file);

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to create table file: " << status.message();
        return status;
    }

    // Compact (merge) file to the newly created table file
    meta::TableFilesSchema updated;

    std::string new_segment_dir;
    utils::GetParentPath(compacted_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    std::string segment_dir_to_merge;
    utils::GetParentPath(file.location_, segment_dir_to_merge);
    segment_writer_ptr->Merge(segment_dir_to_merge, compacted_file.file_id_);

    auto file_to_compact = file;
    file_to_compact.file_type_ = meta::TableFileSchema::TO_DELETE;
    updated.emplace_back(file_to_compact);

    // Serialize
    status = segment_writer_ptr->Serialize();
    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to serialize compacted segment: " << status.message();
        compacted_file.file_type_ = meta::TableFileSchema::TO_DELETE;
        status = meta_ptr_->UpdateTableFile(compacted_file);
        if (status.ok()) {
            ENGINE_LOG_DEBUG << "Mark file: " << compacted_file.file_id_ << " to to_delete";
        }
        return status;
    }

    // Drop index again, in case some files were in the index building process during compaction
    // TODO: might be too frequent?
    DropIndex(table_id);

    // Update table files state
    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
    // else set file type to RAW, no need to build index
    if (compacted_file.engine_type_ != (int)EngineType::FAISS_IDMAP) {
        compacted_file.file_type_ = (segment_writer_ptr->Size() >= compacted_file.index_file_size_)
                                        ? meta::TableFileSchema::TO_INDEX
                                        : meta::TableFileSchema::RAW;
    } else {
        compacted_file.file_type_ = meta::TableFileSchema::RAW;
    }
    compacted_file.file_size_ = segment_writer_ptr->Size();
    compacted_file.row_count_ = segment_writer_ptr->VectorCount();

    if (compacted_file.row_count_ == 0) {
        ENGINE_LOG_DEBUG << "Compacted segment is empty. Mark it as TO_DELETE";
        compacted_file.file_type_ = meta::TableFileSchema::TO_DELETE;
    }

    updated.emplace_back(compacted_file);
    status = meta_ptr_->UpdateTableFiles(updated);

    ENGINE_LOG_DEBUG << "Compacted segment " << compacted_file.segment_id_ << " from "
                     << std::to_string(file_to_compact.file_size_) << " bytes to "
                     << std::to_string(compacted_file.file_size_) << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

Status
DBImpl::GetVectorByID(const std::string& table_id, const IDNumber& vector_id, VectorsData& vector) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    bool has_table;
    auto status = HasTable(table_id, has_table);
    if (!has_table) {
        ENGINE_LOG_ERROR << "Table " << table_id << " does not exist: ";
        return Status(DB_NOT_FOUND, "Table does not exist");
    }
    if (!status.ok()) {
        return status;
    }

    meta::TableFilesSchema files_to_query;

    std::vector<int> file_types{meta::TableFileSchema::FILE_TYPE::RAW, meta::TableFileSchema::FILE_TYPE::TO_INDEX,
                                meta::TableFileSchema::FILE_TYPE::BACKUP};
    meta::TableFilesSchema table_files;
    status = meta_ptr_->FilesByType(table_id, file_types, files_to_query);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files for GetVectorByID: " + status.message();
        ENGINE_LOG_ERROR << err_msg;
        return status;
    }

    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        meta::TableFilesSchema files;
        status = meta_ptr_->FilesByType(schema.table_id_, file_types, files);
        if (!status.ok()) {
            std::string err_msg = "Failed to get files for GetVectorByID: " + status.message();
            ENGINE_LOG_ERROR << err_msg;
            return status;
        }
        files_to_query.insert(files_to_query.end(), std::make_move_iterator(files.begin()),
                              std::make_move_iterator(files.end()));
    }

    if (files_to_query.empty()) {
        ENGINE_LOG_DEBUG << "No files to get vector by id from";
        return Status::OK();
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();
    ongoing_files_checker_.MarkOngoingFiles(files_to_query);

    status = GetVectorByIdHelper(table_id, vector_id, vector, files_to_query);

    ongoing_files_checker_.UnmarkOngoingFiles(files_to_query);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();

    return status;
}

Status
DBImpl::GetVectorByIdHelper(const std::string& table_id, IDNumber vector_id, VectorsData& vector,
                            const meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Getting vector by id in " << files.size() << " files";

    for (auto& file : files) {
        // Load bloom filter
        std::string segment_dir;
        engine::utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

        // Check if the id is present in bloom filter.
        if (id_bloom_filter_ptr->Check(vector_id)) {
            // Load uids and check if the id is indeed present. If yes, find its offset.
            std::vector<int64_t> offsets;
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
                    return status;
                }
                auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

                auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
                if (deleted == deleted_docs.end()) {
                    // Load raw vector
                    bool is_binary = server::ValidationUtil::IsBinaryMetricType(file.metric_type_);
                    size_t single_vector_bytes = is_binary ? file.dimension_ / 8 : file.dimension_ * sizeof(float);
                    std::vector<uint8_t> raw_vector;
                    status = segment_reader.LoadVectors(offset * single_vector_bytes, single_vector_bytes, raw_vector);
                    if (!status.ok()) {
                        return status;
                    }

                    vector.vector_count_ = 1;
                    if (is_binary) {
                        vector.binary_data_ = std::move(raw_vector);
                    } else {
                        std::vector<float> float_vector;
                        float_vector.resize(file.dimension_);
                        memcpy(float_vector.data(), raw_vector.data(), single_vector_bytes);
                        vector.float_data_ = std::move(float_vector);
                    }
                    return Status::OK();
                }
            }
        } else {
            continue;
        }
    }

    return Status::OK();
}

Status
DBImpl::CreateIndex(const std::string& table_id, const TableIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // serialize memory data
    //    std::set<std::string> sync_table_ids;
    //    auto status = SyncMemData(sync_table_ids);
    auto status = Flush();

    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);

        // step 1: check index difference
        TableIndex old_index;
        status = DescribeIndex(table_id, old_index);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to get table index info for table: " << table_id;
            return status;
        }

        // step 2: update index info
        TableIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateTable
        if (!utils::IsSameIndex(old_index, new_index)) {
            status = UpdateTableIndexRecursively(table_id, new_index);
            if (!status.ok()) {
                return status;
            }
        }
    }

    // step 3: let merge file thread finish
    // to avoid duplicate data bug
    WaitMergeFileFinish();

    // step 4: wait and build index
    status = index_failed_checker_.CleanFailedIndexFileOfTable(table_id);
    status = BuildTableIndexRecursively(table_id, index);

    return status;
}

Status
DBImpl::DescribeIndex(const std::string& table_id, TableIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->DescribeTableIndex(table_id, index);
}

Status
DBImpl::DropIndex(const std::string& table_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    ENGINE_LOG_DEBUG << "Drop index for table: " << table_id;
    return DropTableIndexRecursively(table_id);
}

Status
DBImpl::QueryByID(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                  const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nprobe, IDNumber vector_id,
                  ResultIds& result_ids, ResultDistances& result_distances) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    VectorsData vectors_data = VectorsData();
    vectors_data.id_array_.emplace_back(vector_id);
    vectors_data.vector_count_ = 1;
    Status result = Query(context, table_id, partition_tags, k, nprobe, vectors_data, result_ids, result_distances);
    return result;
}

Status
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& table_id,
              const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nprobe, const VectorsData& vectors,
              ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    std::vector<size_t> ids;
    meta::TableFilesSchema files_array;

    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole table
        // get all table files from parent table
        status = GetFilesToSearch(table_id, ids, files_array);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::TableSchema> partition_array;
        status = meta_ptr_->ShowPartitions(table_id, partition_array);
        for (auto& schema : partition_array) {
            status = GetFilesToSearch(schema.table_id_, ids, files_array);
        }

        if (files_array.empty()) {
            return Status::OK();
        }
    } else {
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        GetPartitionsByTags(table_id, partition_tags, partition_name_array);

        for (auto& partition_name : partition_name_array) {
            status = GetFilesToSearch(partition_name, ids, files_array);
        }

        if (files_array.empty()) {
            return Status::OK();
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(query_ctx, table_id, files_array, k, nprobe, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

    return status;
}

Status
DBImpl::QueryByFileID(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                      const std::vector<std::string>& file_ids, uint64_t k, uint64_t nprobe, const VectorsData& vectors,
                      ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query by file id");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // get specified files
    std::vector<size_t> ids;
    for (auto& id : file_ids) {
        meta::TableFileSchema table_file;
        table_file.table_id_ = table_id;
        std::string::size_type sz;
        ids.push_back(std::stoul(id, &sz));
    }

    meta::TableFilesSchema files_array;
    auto status = GetFilesToSearch(table_id, ids, files_array);
    if (!status.ok()) {
        return status;
    }

    if (files_array.empty()) {
        return Status(DB_ERROR, "Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(query_ctx, table_id, files_array, k, nprobe, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

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
DBImpl::QueryAsync(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                   const meta::TableFilesSchema& files, uint64_t k, uint64_t nprobe, const VectorsData& vectors,
                   ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_async_ctx = context->Child("Query Async");

    server::CollectQueryMetrics metrics(vectors.vector_count_);

    TimeRecorder rc("");

    // step 1: construct search job
    auto status = ongoing_files_checker_.MarkOngoingFiles(files);

    ENGINE_LOG_DEBUG << "Engine query begin, index file count: " << files.size();
    scheduler::SearchJobPtr job = std::make_shared<scheduler::SearchJob>(query_async_ctx, k, nprobe, vectors);
    for (auto& file : files) {
        scheduler::TableFileSchemaPtr file_ptr = std::make_shared<meta::TableFileSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    // step 2: put search job to scheduler and wait result
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();

    status = ongoing_files_checker_.UnmarkOngoingFiles(files);
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    // step 3: construct results
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

    query_async_ctx->GetTraceContext()->GetSpan()->Finish();

    return Status::OK();
}

void
DBImpl::BackgroundTimerTask() {
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            WaitMergeFileFinish();
            WaitBuildIndexFinish();

            ENGINE_LOG_DEBUG << "DB background thread exit";
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(options_.auto_flush_interval_));

        StartMetricTask();
        StartCompactionTask();
        StartBuildIndexTask();
    }
}

void
DBImpl::WaitMergeFileFinish() {
    std::lock_guard<std::mutex> lck(compact_result_mutex_);
    for (auto& iter : compact_thread_results_) {
        iter.wait();
    }
}

void
DBImpl::WaitBuildIndexFinish() {
    std::lock_guard<std::mutex> lck(index_result_mutex_);
    for (auto& iter : index_thread_results_) {
        iter.wait();
    }
}

void
DBImpl::StartMetricTask() {
    static uint64_t metric_clock_tick = 0;
    ++metric_clock_tick;
    if (metric_clock_tick % METRIC_ACTION_INTERVAL != 0) {
        return;
    }

    // ENGINE_LOG_TRACE << "Start metric task";

    server::Metrics::GetInstance().KeepingAliveCounterIncrement(METRIC_ACTION_INTERVAL);
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
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

    // ENGINE_LOG_TRACE << "Metric task finished";
}

/*

Status
DBImpl::SyncMemData(std::set<std::string>& sync_table_ids) {
    std::lock_guard<std::mutex> lck(mem_serialize_mutex_);
    std::set<std::string> temp_table_ids;
    mem_mgr_->Serialize(temp_table_ids);
    for (auto& id : temp_table_ids) {
        sync_table_ids.insert(id);
    }

    if (!temp_table_ids.empty()) {
        SERVER_LOG_DEBUG << "Insert cache serialized";
    }

    return Status::OK();
}

 */

void
DBImpl::StartCompactionTask() {
    static uint64_t compact_clock_tick = 0;
    ++compact_clock_tick;
    if (compact_clock_tick % COMPACT_ACTION_INTERVAL != 0) {
        return;
    }

    if (!options_.wal_enable_) {
        Flush();
    }

    // compaction has been finished?
    {
        std::lock_guard<std::mutex> lck(compact_result_mutex_);
        if (!compact_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (compact_thread_results_.back().wait_for(span) == std::future_status::ready) {
                compact_thread_results_.pop_back();
            }
        }
    }

    // add new compaction task
    {
        std::lock_guard<std::mutex> lck(compact_result_mutex_);
        if (compact_thread_results_.empty()) {
            // collect merge files for all tables(if compact_table_ids_ is empty) for two reasons:
            // 1. other tables may still has un-merged files
            // 2. server may be closed unexpected, these un-merge files need to be merged when server restart
            if (compact_table_ids_.empty()) {
                std::vector<meta::TableSchema> table_schema_array;
                meta_ptr_->AllTables(table_schema_array);
                for (auto& schema : table_schema_array) {
                    compact_table_ids_.insert(schema.table_id_);
                }
            }

            // start merge file thread
            compact_thread_results_.push_back(
                compact_thread_pool_.enqueue(&DBImpl::BackgroundCompaction, this, compact_table_ids_));
            compact_table_ids_.clear();
        }
    }
}

Status
DBImpl::MergeFiles(const std::string& table_id, const meta::TableFilesSchema& files) {
    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

    ENGINE_LOG_DEBUG << "Merge files for table: " << table_id;

    // step 1: create table file
    meta::TableFileSchema table_file;
    table_file.table_id_ = table_id;
    table_file.file_type_ = meta::TableFileSchema::NEW_MERGE;
    Status status = meta_ptr_->CreateTableFile(table_file);

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to create table: " << status.ToString();
        return status;
    }

    // step 2: merge files
    /*
    ExecutionEnginePtr index =
        EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_,
                             (MetricType)table_file.metric_type_, table_file.nlist_);
*/
    meta::TableFilesSchema updated;

    std::string new_segment_dir;
    utils::GetParentPath(table_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    for (auto& file : files) {
        server::CollectMergeFilesMetrics metrics;
        std::string segment_dir_to_merge;
        utils::GetParentPath(file.location_, segment_dir_to_merge);
        segment_writer_ptr->Merge(segment_dir_to_merge, table_file.file_id_);
        auto file_schema = file;
        file_schema.file_type_ = meta::TableFileSchema::TO_DELETE;
        updated.push_back(file_schema);
        auto size = segment_writer_ptr->Size();
        if (size >= file_schema.index_file_size_) {
            break;
        }
    }

    // step 3: serialize to disk
    try {
        status = segment_writer_ptr->Serialize();
    } catch (std::exception& ex) {
        std::string msg = "Serialize merged index encounter exception: " + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;
        status = Status(DB_ERROR, msg);
    }

    if (!status.ok()) {
        // if failed to serialize merge file to disk
        // typical error: out of disk space, out of memory or permition denied
        table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
        status = meta_ptr_->UpdateTableFile(table_file);
        ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

        ENGINE_LOG_ERROR << "Failed to persist merged segment: " << new_segment_dir << ". Error: " << status.message();

        return status;
    }

    // step 4: update table files state
    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
    // else set file type to RAW, no need to build index
    if (table_file.engine_type_ != (int)EngineType::FAISS_IDMAP) {
        table_file.file_type_ = (segment_writer_ptr->Size() >= table_file.index_file_size_)
                                    ? meta::TableFileSchema::TO_INDEX
                                    : meta::TableFileSchema::RAW;
    } else {
        table_file.file_type_ = meta::TableFileSchema::RAW;
    }
    table_file.file_size_ = segment_writer_ptr->Size();
    table_file.row_count_ = segment_writer_ptr->VectorCount();
    updated.push_back(table_file);
    status = meta_ptr_->UpdateTableFiles(updated);
    ENGINE_LOG_DEBUG << "New merged segment " << table_file.segment_id_ << " of size " << segment_writer_ptr->Size()
                     << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

Status
DBImpl::BackgroundMergeFiles(const std::string& table_id) {
    // const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

    meta::TableFilesSchema raw_files;
    auto status = meta_ptr_->FilesToMerge(table_id, raw_files);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to get merge files for table: " << table_id;
        return status;
    }

    if (raw_files.size() < options_.merge_trigger_number_) {
        ENGINE_LOG_TRACE << "Files number not greater equal than merge trigger number, skip merge action";
        return Status::OK();
    }

    status = ongoing_files_checker_.MarkOngoingFiles(raw_files);
    MergeFiles(table_id, raw_files);
    status = ongoing_files_checker_.UnmarkOngoingFiles(raw_files);

    if (!initialized_.load(std::memory_order_acquire)) {
        ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action for table: " << table_id;
    }

    return Status::OK();
}

void
DBImpl::BackgroundCompaction(std::set<std::string> table_ids) {
    // ENGINE_LOG_TRACE << " Background compaction thread start";

    Status status;
    for (auto& table_id : table_ids) {
        status = BackgroundMergeFiles(table_id);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Merge files for table " << table_id << " failed: " << status.ToString();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action";
            break;
        }
    }

    meta_ptr_->Archive();

    {
        uint64_t ttl = 10 * meta::SECOND;  // default: file will be hard-deleted few seconds after soft-deleted
        if (options_.mode_ == DBOptions::MODE::CLUSTER_WRITABLE) {
            ttl = meta::HOUR;
        }

        meta_ptr_->CleanUpFilesWithTTL(ttl, &ongoing_files_checker_);
    }

    // ENGINE_LOG_TRACE << " Background compaction thread exit";
}

void
DBImpl::StartBuildIndexTask(bool force) {
    static uint64_t index_clock_tick = 0;
    ++index_clock_tick;
    if (!force && (index_clock_tick % INDEX_ACTION_INTERVAL != 0)) {
        return;
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
            index_thread_results_.push_back(index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndex, this));
        }
    }
}

void
DBImpl::BackgroundBuildIndex() {
    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::TableFilesSchema to_index_files;
    meta_ptr_->FilesToIndex(to_index_files);
    Status status = index_failed_checker_.IgnoreFailedIndexFiles(to_index_files);

    if (!to_index_files.empty()) {
        ENGINE_LOG_DEBUG << "Background build index thread begin";
        status = ongoing_files_checker_.MarkOngoingFiles(to_index_files);

        // step 2: put build index task to scheduler
        std::vector<std::pair<scheduler::BuildIndexJobPtr, scheduler::TableFileSchemaPtr>> job2file_map;
        for (auto& file : to_index_files) {
            scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(meta_ptr_, options_);
            scheduler::TableFileSchemaPtr file_ptr = std::make_shared<meta::TableFileSchema>(file);
            job->AddToIndexFiles(file_ptr);
            scheduler::JobMgrInst::GetInstance()->Put(job);
            job2file_map.push_back(std::make_pair(job, file_ptr));
        }

        // step 3: wait build index finished and mark failed files
        for (auto iter = job2file_map.begin(); iter != job2file_map.end(); ++iter) {
            scheduler::BuildIndexJobPtr job = iter->first;
            meta::TableFileSchema& file_schema = *(iter->second.get());
            job->WaitBuildIndexFinish();
            if (!job->GetStatus().ok()) {
                Status status = job->GetStatus();
                ENGINE_LOG_ERROR << "Building index job " << job->id() << " failed: " << status.ToString();

                index_failed_checker_.MarkFailedIndexFile(file_schema);
            } else {
                ENGINE_LOG_DEBUG << "Building index job " << job->id() << " succeed.";

                index_failed_checker_.MarkSucceedIndexFile(file_schema);
            }
            status = ongoing_files_checker_.UnmarkOngoingFile(file_schema);
        }

        ENGINE_LOG_DEBUG << "Background build index thread finished";
    }
}

Status
DBImpl::GetFilesToBuildIndex(const std::string& table_id, const std::vector<int>& file_types,
                             meta::TableFilesSchema& files) {
    files.clear();
    auto status = meta_ptr_->FilesByType(table_id, file_types, files);

    // only build index for files that row count greater than certain threshold
    for (auto it = files.begin(); it != files.end();) {
        if ((*it).file_type_ == static_cast<int>(meta::TableFileSchema::RAW) &&
            (*it).row_count_ < meta::BUILD_INDEX_THRESHOLD) {
            it = files.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

Status
DBImpl::GetFilesToSearch(const std::string& table_id, const std::vector<size_t>& file_ids,
                         meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Collect files from table: " << table_id;

    meta::TableFilesSchema search_files;
    auto status = meta_ptr_->FilesToSearch(table_id, file_ids, search_files);
    if (!status.ok()) {
        return status;
    }

    for (auto& file : search_files) {
        files.push_back(file);
    }
    return Status::OK();
}

Status
DBImpl::GetPartitionByTag(const std::string& table_id, const std::string& partition_tag, std::string& partition_name) {
    Status status;

    if (partition_tag.empty()) {
        partition_name = table_id;

    } else {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = partition_tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name = table_id;
            return status;
        }

        status = meta_ptr_->GetPartitionName(table_id, partition_tag, partition_name);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << status.message();
        }
    }

    return status;
}

Status
DBImpl::GetPartitionsByTags(const std::string& table_id, const std::vector<std::string>& partition_tags,
                            std::set<std::string>& partition_name_array) {
    std::vector<meta::TableSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(table_id, partition_array);

    for (auto& tag : partition_tags) {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name_array.insert(table_id);
            return status;
        }

        for (auto& schema : partition_array) {
            if (server::StringHelpFunctions::IsRegexMatch(schema.partition_tag_, valid_tag)) {
                partition_name_array.insert(schema.table_id_);
            }
        }
    }

    return Status::OK();
}

Status
DBImpl::DropTableRecursively(const std::string& table_id) {
    // dates partly delete files of the table but currently we don't support
    ENGINE_LOG_DEBUG << "Prepare to delete table " << table_id;

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DropTable(table_id);
    }

    status = mem_mgr_->EraseMemVector(table_id);  // not allow insert
    status = meta_ptr_->DropTable(table_id);      // soft delete table
    index_failed_checker_.CleanFailedIndexFileOfTable(table_id);

    // scheduler will determine when to delete table files
    auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
    scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(table_id, meta_ptr_, nres);
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitAndDelete();

    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropTableRecursively(schema.table_id_);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::UpdateTableIndexRecursively(const std::string& table_id, const TableIndex& index) {
    DropIndex(table_id);

    auto status = meta_ptr_->UpdateTableIndex(table_id, index);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to update table index info for table: " << table_id;
        return status;
    }

    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = UpdateTableIndexRecursively(schema.table_id_, index);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::BuildTableIndexRecursively(const std::string& table_id, const TableIndex& index) {
    // for IDMAP type, only wait all NEW file converted to RAW file
    // for other type, wait NEW/RAW/NEW_MERGE/NEW_INDEX/TO_INDEX files converted to INDEX files
    std::vector<int> file_types;
    if (index.engine_type_ == static_cast<int32_t>(EngineType::FAISS_IDMAP)) {
        file_types = {
            static_cast<int32_t>(meta::TableFileSchema::NEW),
            static_cast<int32_t>(meta::TableFileSchema::NEW_MERGE),
        };
    } else {
        file_types = {
            static_cast<int32_t>(meta::TableFileSchema::RAW),
            static_cast<int32_t>(meta::TableFileSchema::NEW),
            static_cast<int32_t>(meta::TableFileSchema::NEW_MERGE),
            static_cast<int32_t>(meta::TableFileSchema::NEW_INDEX),
            static_cast<int32_t>(meta::TableFileSchema::TO_INDEX),
        };
    }

    // get files to build index
    meta::TableFilesSchema table_files;
    auto status = GetFilesToBuildIndex(table_id, file_types, table_files);
    int times = 1;

    while (!table_files.empty()) {
        ENGINE_LOG_DEBUG << "Non index files detected! Will build index " << times;
        if (index.engine_type_ != (int)EngineType::FAISS_IDMAP) {
            status = meta_ptr_->UpdateTableFilesToIndex(table_id);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(10 * 1000, times * 100)));
        GetFilesToBuildIndex(table_id, file_types, table_files);
        ++times;

        index_failed_checker_.IgnoreFailedIndexFiles(table_files);
    }

    // build index for partition
    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = BuildTableIndexRecursively(schema.table_id_, index);
        if (!status.ok()) {
            return status;
        }
    }

    // failed to build index for some files, return error
    std::vector<std::string> failed_files;
    index_failed_checker_.GetFailedIndexFileOfTable(table_id, failed_files);
    if (!failed_files.empty()) {
        std::string msg = "Failed to build index for " + std::to_string(failed_files.size()) +
                          ((failed_files.size() == 1) ? " file" : " files");
        msg += ", please double check index parameters.";
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
DBImpl::DropTableIndexRecursively(const std::string& table_id) {
    ENGINE_LOG_DEBUG << "Drop index for table: " << table_id;
    index_failed_checker_.CleanFailedIndexFileOfTable(table_id);
    auto status = meta_ptr_->DropTableIndex(table_id);
    if (!status.ok()) {
        return status;
    }

    // drop partition index
    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropTableIndexRecursively(schema.table_id_);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::GetTableRowCountRecursively(const std::string& table_id, uint64_t& row_count) {
    row_count = 0;
    auto status = meta_ptr_->Count(table_id, row_count);
    if (!status.ok()) {
        return status;
    }

    // get partition row count
    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        uint64_t partition_row_count = 0;
        status = GetTableRowCountRecursively(schema.table_id_, partition_row_count);
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

    auto tables_flushed = [&](const std::set<std::string>& table_ids) -> uint64_t {
        if (table_ids.empty()) {
            return 0;
        }

        uint64_t max_lsn = 0;
        if (options_.wal_enable_) {
            for (auto& table : table_ids) {
                uint64_t lsn = 0;
                meta_ptr_->GetTableFlushLSN(table, lsn);
                wal_mgr_->TableFlushed(table, lsn);
                if (lsn > max_lsn) {
                    max_lsn = lsn;
                }
            }
        }

        std::lock_guard<std::mutex> lck(compact_result_mutex_);
        for (auto& table : table_ids) {
            compact_table_ids_.insert(table);
        }
        return max_lsn;
    };

    Status status;

    switch (record.type) {
        case wal::MXLogType::InsertBinary: {
            std::string target_table_name;
            status = GetPartitionByTag(record.table_id, record.partition_tag, target_table_name);
            if (!status.ok()) {
                return status;
            }

            std::set<std::string> flushed_tables;
            status = mem_mgr_->InsertVectors(target_table_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(uint8_t)),
                                             (const u_int8_t*)record.data, record.lsn, flushed_tables);
            // even though !status.ok, run
            tables_flushed(flushed_tables);

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::InsertVector: {
            std::string target_table_name;
            status = GetPartitionByTag(record.table_id, record.partition_tag, target_table_name);
            if (!status.ok()) {
                return status;
            }

            std::set<std::string> flushed_tables;
            status = mem_mgr_->InsertVectors(target_table_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(float)),
                                             (const float*)record.data, record.lsn, flushed_tables);
            // even though !status.ok, run
            tables_flushed(flushed_tables);

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::Delete: {
            if (record.length == 1) {
                status = mem_mgr_->DeleteVector(record.table_id, *record.ids, record.lsn);
            } else {
                status = mem_mgr_->DeleteVectors(record.table_id, record.length, record.ids, record.lsn);
            }
            break;
        }

        case wal::MXLogType::Flush: {
            if (!record.table_id.empty()) {
                // flush one table
                bool has_table;
                auto status = HasTable(record.table_id, has_table);
                if (!status.ok()) {
                    return Status(DB_ERROR, status.message());
                }
                if (!has_table) {
                    ENGINE_LOG_ERROR << "Table to flush does not exist: " << record.table_id;
                    return Status(DB_NOT_FOUND, "Table to flush does not exist");
                }

                {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(record.table_id);
                }

                std::set<std::string> flushed_tables;
                flushed_tables.insert(record.table_id);
                tables_flushed(flushed_tables);

            } else {
                // flush all tables
                std::set<std::string> table_ids;
                {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(table_ids);
                }

                uint64_t lsn = tables_flushed(table_ids);
                if (options_.wal_enable_) {
                    wal_mgr_->RemoveOldFiles(lsn);
                }
            }
            break;
        }
    }

    return status;
}

void
DBImpl::BackgroundWalTask() {
    server::SystemInfo::GetInstance().Init();

    auto get_next_auto_flush_time = [&]() {
        return std::chrono::system_clock::now() + std::chrono::milliseconds(options_.auto_flush_interval_);
    };
    auto next_auto_flush_time = get_next_auto_flush_time();

    wal::MXLogRecord record;

    auto auto_flush = [&]() {
        record.type = wal::MXLogType::Flush;
        record.table_id.clear();
        ExecWalRecord(record);

        StartMetricTask();
        StartCompactionTask();
        StartBuildIndexTask();
    };

    while (true) {
        if (std::chrono::system_clock::now() >= next_auto_flush_time) {
            auto_flush();
            next_auto_flush_time = get_next_auto_flush_time();
        }

        auto error_code = wal_mgr_->GetNextRecord(record);
        if (error_code != WAL_SUCCESS) {
            ENGINE_LOG_ERROR << "WAL background GetNextRecord error";
            break;
        }

        if (record.type != wal::MXLogType::None) {
            ExecWalRecord(record);
            if (record.type == wal::MXLogType::Flush) {
                // user req flush
                flush_task_swn_.Notify();

                // if user flush all manually, update auto flush also
                if (record.table_id.empty()) {
                    next_auto_flush_time = get_next_auto_flush_time();
                }
            }

        } else {
            if (!initialized_.load(std::memory_order_acquire)) {
                auto_flush();
                WaitMergeFileFinish();
                WaitBuildIndexFinish();
                ENGINE_LOG_DEBUG << "WAL background thread exit";
                break;
            }

            wal_task_swn_.Wait_Until(next_auto_flush_time);
        }
    }
}

}  // namespace engine
}  // namespace milvus
