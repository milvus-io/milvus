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

#include <algorithm>
#include <boost/filesystem.hpp>
#include <chrono>
#include <cstring>
#include <iostream>
#include <set>
#include <thread>
#include <utility>

#include "Utils.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
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
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

namespace {

constexpr uint64_t METRIC_ACTION_INTERVAL = 1;
constexpr uint64_t COMPACT_ACTION_INTERVAL = 1;
constexpr uint64_t INDEX_ACTION_INTERVAL = 1;

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");

void
TraverseFiles(const meta::DatePartionedTableFilesSchema& date_files, meta::TableFilesSchema& files_array) {
    for (auto& day_files : date_files) {
        for (auto& file : day_files.second) {
            files_array.push_back(file);
        }
    }
}

}  // namespace

DBImpl::DBImpl(const DBOptions& options)
    : options_(options), shutting_down_(true), compact_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    meta_ptr_ = MetaFactory::Build(options.meta_, options.mode_);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);
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
    if (!shutting_down_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // ENGINE_LOG_TRACE << "DB service start";
    shutting_down_.store(false, std::memory_order_release);

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // ENGINE_LOG_TRACE << "StartTimerTasks";
        bg_timer_thread_ = std::thread(&DBImpl::BackgroundTimerTask, this);
    }

    return Status::OK();
}

Status
DBImpl::Stop() {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    shutting_down_.store(true, std::memory_order_release);

    // makesure all memory data serialized
    std::set<std::string> sync_table_ids;
    SyncMemData(sync_table_ids);

    // wait compaction/buildindex finish
    bg_timer_thread_.join();

    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
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
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::TableSchema temp_schema = table_schema;
    temp_schema.index_file_size_ *= ONE_MB;  // store as MB
    return meta_ptr_->CreateTable(temp_schema);
}

Status
DBImpl::DropTable(const std::string& table_id, const meta::DatesT& dates) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return DropTableRecursively(table_id, dates);
}

Status
DBImpl::DescribeTable(meta::TableSchema& table_schema) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto stat = meta_ptr_->DescribeTable(table_schema);
    table_schema.index_file_size_ /= ONE_MB;  // return as MB
    return stat;
}

Status
DBImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasTable(table_id, has_or_not);
}

Status
DBImpl::AllTables(std::vector<meta::TableSchema>& table_schema_array) {
    if (shutting_down_.load(std::memory_order_acquire)) {
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
DBImpl::PreloadTable(const std::string& table_id) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: get all table files from parent table
    meta::DatesT dates;
    std::vector<size_t> ids;
    meta::TableFilesSchema files_array;
    auto status = GetFilesToSearch(table_id, ids, dates, files_array);
    if (!status.ok()) {
        return status;
    }

    // step 2: get files from partition tables
    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = GetFilesToSearch(schema.table_id_, ids, dates, files_array);
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
        ExecutionEnginePtr engine = EngineFactory::Build(file.dimension_, file.location_, (EngineType)file.engine_type_,
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
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->UpdateTableFlag(table_id, flag);
}

Status
DBImpl::GetTableRowCount(const std::string& table_id, uint64_t& row_count) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return GetTableRowCountRecursively(table_id, row_count);
}

Status
DBImpl::CreatePartition(const std::string& table_id, const std::string& partition_name,
                        const std::string& partition_tag) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->CreatePartition(table_id, partition_name, partition_tag);
}

Status
DBImpl::DropPartition(const std::string& partition_name) {
    if (shutting_down_.load(std::memory_order_acquire)) {
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
    if (shutting_down_.load(std::memory_order_acquire)) {
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
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->ShowPartitions(table_id, partition_schema_array);
}

Status
DBImpl::InsertVectors(const std::string& table_id, const std::string& partition_tag, uint64_t n, const float* vectors,
                      IDNumbers& vector_ids) {
    //    ENGINE_LOG_DEBUG << "Insert " << n << " vectors to cache";
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // if partition is specified, use partition as target table
    Status status;
    std::string target_table_name = table_id;
    if (!partition_tag.empty()) {
        std::string partition_name;
        status = meta_ptr_->GetPartitionName(table_id, partition_tag, target_table_name);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << status.message();
            return status;
        }
    }

    // insert vectors into target table
    milvus::server::CollectInsertMetrics metrics(n, status);
    status = mem_mgr_->InsertVectors(target_table_name, n, vectors, vector_ids);

    return status;
}

Status
DBImpl::CreateIndex(const std::string& table_id, const TableIndex& index) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // serialize memory data
    std::set<std::string> sync_table_ids;
    auto status = SyncMemData(sync_table_ids);

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
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->DescribeTableIndex(table_id, index);
}

Status
DBImpl::DropIndex(const std::string& table_id) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    ENGINE_LOG_DEBUG << "Drop index for table: " << table_id;
    return DropTableIndexRecursively(table_id);
}

Status
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& table_id,
              const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nq, uint64_t nprobe,
              const float* vectors, ResultIds& result_ids, ResultDistances& result_distances) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::DatesT dates = {utils::GetDate()};
    Status result =
        Query(context, table_id, partition_tags, k, nq, nprobe, vectors, dates, result_ids, result_distances);
    return result;
}

Status
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& table_id,
              const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nq, uint64_t nprobe,
              const float* vectors, const meta::DatesT& dates, ResultIds& result_ids,
              ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query");

    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    ENGINE_LOG_DEBUG << "Query by dates for table: " << table_id << " date range count: " << dates.size();

    Status status;
    std::vector<size_t> ids;
    meta::TableFilesSchema files_array;

    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole table
        // get all table files from parent table
        status = GetFilesToSearch(table_id, ids, dates, files_array);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::TableSchema> partition_array;
        status = meta_ptr_->ShowPartitions(table_id, partition_array);
        for (auto& schema : partition_array) {
            status = GetFilesToSearch(schema.table_id_, ids, dates, files_array);
        }
    } else {
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        GetPartitionsByTags(table_id, partition_tags, partition_name_array);

        for (auto& partition_name : partition_name_array) {
            status = GetFilesToSearch(partition_name, ids, dates, files_array);
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(query_ctx, table_id, files_array, k, nq, nprobe, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

    return status;
}

Status
DBImpl::QueryByFileID(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                      const std::vector<std::string>& file_ids, uint64_t k, uint64_t nq, uint64_t nprobe,
                      const float* vectors, const meta::DatesT& dates, ResultIds& result_ids,
                      ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query by file id");

    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    ENGINE_LOG_DEBUG << "Query by file ids for table: " << table_id << " date range count: " << dates.size();

    // get specified files
    std::vector<size_t> ids;
    for (auto& id : file_ids) {
        meta::TableFileSchema table_file;
        table_file.table_id_ = table_id;
        std::string::size_type sz;
        ids.push_back(std::stoul(id, &sz));
    }

    meta::TableFilesSchema files_array;
    auto status = GetFilesToSearch(table_id, ids, dates, files_array);
    if (!status.ok()) {
        return status;
    }

    if (files_array.empty()) {
        return Status(DB_ERROR, "Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(query_ctx, table_id, files_array, k, nq, nprobe, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

    return status;
}

Status
DBImpl::Size(uint64_t& result) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->Size(result);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// internal methods
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::QueryAsync(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                   const meta::TableFilesSchema& files, uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
                   ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_async_ctx = context->Child("Query Async");

    server::CollectQueryMetrics metrics(nq);

    TimeRecorder rc("");

    // step 1: construct search job
    auto status = ongoing_files_checker_.MarkOngoingFiles(files);

    ENGINE_LOG_DEBUG << "Engine query begin, index file count: " << files.size();
    scheduler::SearchJobPtr job = std::make_shared<scheduler::SearchJob>(query_async_ctx, k, nq, nprobe, vectors);
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
    Status status;
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (shutting_down_.load(std::memory_order_acquire)) {
            WaitMergeFileFinish();
            WaitBuildIndexFinish();

            ENGINE_LOG_DEBUG << "DB background thread exit";
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

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
    metric_clock_tick++;
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

    // ENGINE_LOG_TRACE << "Metric task finished";
}

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

void
DBImpl::StartCompactionTask() {
    static uint64_t compact_clock_tick = 0;
    compact_clock_tick++;
    if (compact_clock_tick % COMPACT_ACTION_INTERVAL != 0) {
        return;
    }

    // serialize memory data
    SyncMemData(compact_table_ids_);

    // compactiong has been finished?
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
DBImpl::MergeFiles(const std::string& table_id, const meta::DateT& date, const meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Merge files for table: " << table_id;

    // step 1: create table file
    meta::TableFileSchema table_file;
    table_file.table_id_ = table_id;
    table_file.date_ = date;
    table_file.file_type_ = meta::TableFileSchema::NEW_MERGE;
    Status status = meta_ptr_->CreateTableFile(table_file);

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to create table: " << status.ToString();
        return status;
    }

    // step 2: merge files
    ExecutionEnginePtr index =
        EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_,
                             (MetricType)table_file.metric_type_, table_file.nlist_);

    meta::TableFilesSchema updated;
    int64_t index_size = 0;

    for (auto& file : files) {
        server::CollectMergeFilesMetrics metrics;

        index->Merge(file.location_);
        auto file_schema = file;
        file_schema.file_type_ = meta::TableFileSchema::TO_DELETE;
        updated.push_back(file_schema);
        index_size = index->Size();

        if (index_size >= file_schema.index_file_size_) {
            break;
        }
    }

    // step 3: serialize to disk
    try {
        status = index->Serialize();
        if (!status.ok()) {
            ENGINE_LOG_ERROR << status.message();
        }
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

        ENGINE_LOG_ERROR << "Failed to persist merged file: " << table_file.location_
                         << ", possible out of disk space or memory";

        return status;
    }

    // step 4: update table files state
    // if index type isn't IDMAP, set file type to TO_INDEX if file size execeed index_file_size
    // else set file type to RAW, no need to build index
    if (table_file.engine_type_ != (int)EngineType::FAISS_IDMAP) {
        table_file.file_type_ = (index->PhysicalSize() >= table_file.index_file_size_) ? meta::TableFileSchema::TO_INDEX
                                                                                       : meta::TableFileSchema::RAW;
    } else {
        table_file.file_type_ = meta::TableFileSchema::RAW;
    }
    table_file.file_size_ = index->PhysicalSize();
    table_file.row_count_ = index->Count();
    updated.push_back(table_file);
    status = meta_ptr_->UpdateTableFiles(updated);
    ENGINE_LOG_DEBUG << "New merged file " << table_file.file_id_ << " of size " << index->PhysicalSize() << " bytes";

    if (options_.insert_cache_immediately_) {
        index->Cache();
    }

    return status;
}

Status
DBImpl::BackgroundMergeFiles(const std::string& table_id) {
    meta::DatePartionedTableFilesSchema raw_files;
    auto status = meta_ptr_->FilesToMerge(table_id, raw_files);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to get merge files for table: " << table_id;
        return status;
    }

    for (auto& kv : raw_files) {
        meta::TableFilesSchema& files = kv.second;
        if (files.size() < options_.merge_trigger_number_) {
            ENGINE_LOG_TRACE << "Files number not greater equal than merge trigger number, skip merge action";
            continue;
        }

        status = ongoing_files_checker_.MarkOngoingFiles(files);
        MergeFiles(table_id, kv.first, kv.second);
        status = ongoing_files_checker_.UnmarkOngoingFiles(files);

        if (shutting_down_.load(std::memory_order_acquire)) {
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action for table: " << table_id;
            break;
        }
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

        if (shutting_down_.load(std::memory_order_acquire)) {
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
    index_clock_tick++;
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
            it++;
        }
    }

    return Status::OK();
}

Status
DBImpl::GetFilesToSearch(const std::string& table_id, const std::vector<size_t>& file_ids, const meta::DatesT& dates,
                         meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Collect files from table: " << table_id;

    meta::DatePartionedTableFilesSchema date_files;
    auto status = meta_ptr_->FilesToSearch(table_id, file_ids, dates, date_files);
    if (!status.ok()) {
        return status;
    }

    TraverseFiles(date_files, files);
    return Status::OK();
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
        for (auto& schema : partition_array) {
            if (server::StringHelpFunctions::IsRegexMatch(schema.partition_tag_, valid_tag)) {
                partition_name_array.insert(schema.table_id_);
            }
        }
    }

    return Status::OK();
}

Status
DBImpl::DropTableRecursively(const std::string& table_id, const meta::DatesT& dates) {
    // dates partly delete files of the table but currently we don't support
    ENGINE_LOG_DEBUG << "Prepare to delete table " << table_id;

    Status status;
    if (dates.empty()) {
        status = mem_mgr_->EraseMemVector(table_id);  // not allow insert
        status = meta_ptr_->DropTable(table_id);      // soft delete table
        index_failed_checker_.CleanFailedIndexFileOfTable(table_id);

        // scheduler will determine when to delete table files
        auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
        scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(table_id, meta_ptr_, nres);
        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitAndDelete();
    } else {
        status = meta_ptr_->DropDataByDate(table_id, dates);
    }

    std::vector<meta::TableSchema> partition_array;
    status = meta_ptr_->ShowPartitions(table_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropTableRecursively(schema.table_id_, dates);
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
        times++;

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

}  // namespace engine
}  // namespace milvus
