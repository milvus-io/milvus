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

#include "DBImpl.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "engine/EngineFactory.h"
#include "insert/MemMenagerFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "meta/MetaFactory.h"
#include "meta/MetaConsts.h"
#include "metrics/Metrics.h"
#include "scheduler/job/SearchJob.h"
#include "scheduler/job/DeleteJob.h"
#include "scheduler/SchedInst.h"
#include "utils/TimeRecorder.h"
#include "utils/Log.h"
#include "Utils.h"

#include <assert.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <boost/filesystem.hpp>

namespace zilliz {
namespace milvus {
namespace engine {

namespace {

constexpr uint64_t METRIC_ACTION_INTERVAL = 1;
constexpr uint64_t COMPACT_ACTION_INTERVAL = 1;
constexpr uint64_t INDEX_ACTION_INTERVAL = 1;

}


DBImpl::DBImpl(const DBOptions& options)
    : options_(options),
      shutting_down_(true),
      compact_thread_pool_(1, 1),
      index_thread_pool_(1, 1) {
    meta_ptr_ = MetaFactory::Build(options.meta_, options.mode_);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);
    Start();
}

DBImpl::~DBImpl() {
    Stop();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//external api
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status DBImpl::Start() {
    if (!shutting_down_.load(std::memory_order_acquire)){
        return Status::OK();
    }

    ENGINE_LOG_TRACE << "DB service start";
    shutting_down_.store(false, std::memory_order_release);

    //for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::READ_ONLY) {
        ENGINE_LOG_TRACE << "StartTimerTasks";
        bg_timer_thread_ = std::thread(&DBImpl::BackgroundTimerTask, this);
    }

    return Status::OK();
}

Status DBImpl::Stop() {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status::OK();
    }

    shutting_down_.store(true, std::memory_order_release);

    //makesure all memory data serialized
    MemSerialize();

    //wait compaction/buildindex finish
    bg_timer_thread_.join();

    if (options_.mode_ != DBOptions::MODE::READ_ONLY) {
        meta_ptr_->CleanUp();
    }

    ENGINE_LOG_TRACE << "DB service stop";
    return Status::OK();
}

Status DBImpl::DropAll() {
    return meta_ptr_->DropAll();
}

Status DBImpl::CreateTable(meta::TableSchema& table_schema) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    meta::TableSchema temp_schema = table_schema;
    temp_schema.index_file_size_ *= ONE_MB; //store as MB
    return meta_ptr_->CreateTable(temp_schema);
}

Status DBImpl::DeleteTable(const std::string& table_id, const meta::DatesT& dates) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    //dates partly delete files of the table but currently we don't support
    ENGINE_LOG_DEBUG << "Prepare to delete table " << table_id;

    if (dates.empty()) {
        mem_mgr_->EraseMemVector(table_id); //not allow insert
        meta_ptr_->DeleteTable(table_id); //soft delete table

        //scheduler will determine when to delete table files
        auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
        scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(0, table_id, meta_ptr_, nres);
        scheduler::JobMgrInst::GetInstance()->Put(job);
        job->WaitAndDelete();
    } else {
        meta_ptr_->DropPartitionsByDates(table_id, dates);
    }

    return Status::OK();
}

Status DBImpl::DescribeTable(meta::TableSchema& table_schema) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    auto stat = meta_ptr_->DescribeTable(table_schema);
    table_schema.index_file_size_ /= ONE_MB; //return as MB
    return stat;
}

Status DBImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    return meta_ptr_->HasTable(table_id, has_or_not);
}

Status DBImpl::AllTables(std::vector<meta::TableSchema>& table_schema_array) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    return meta_ptr_->AllTables(table_schema_array);
}

Status DBImpl::PreloadTable(const std::string &table_id) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    meta::DatePartionedTableFilesSchema files;

    meta::DatesT dates;
    std::vector<size_t> ids;
    auto status = meta_ptr_->FilesToSearch(table_id, ids, dates, files);
    if (!status.ok()) {
        return status;
    }

    int64_t size = 0;
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t available_size = cache_total - cache_usage;

    for(auto &day_files : files) {
        for (auto &file : day_files.second) {
            ExecutionEnginePtr engine = EngineFactory::Build(file.dimension_, file.location_, (EngineType)file.engine_type_, (MetricType)file.metric_type_, file.nlist_);
            if(engine == nullptr) {
                ENGINE_LOG_ERROR << "Invalid engine type";
                return Status(DB_ERROR, "Invalid engine type");
            }

            size += engine->PhysicalSize();
            if (size > available_size) {
                break;
            } else {
                try {
                    //step 1: load index
                    engine->Load(true);
                } catch (std::exception &ex) {
                    std::string msg = "Pre-load table encounter exception: " + std::string(ex.what());
                    ENGINE_LOG_ERROR << msg;
                    return Status(DB_ERROR, msg);
                }
            }
        }
    }
    return Status::OK();
}

Status DBImpl::UpdateTableFlag(const std::string &table_id, int64_t flag) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    return meta_ptr_->UpdateTableFlag(table_id, flag);
}

Status DBImpl::GetTableRowCount(const std::string& table_id, uint64_t& row_count) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    return meta_ptr_->Count(table_id, row_count);
}

Status DBImpl::InsertVectors(const std::string& table_id_,
        uint64_t n, const float* vectors, IDNumbers& vector_ids_) {
//    ENGINE_LOG_DEBUG << "Insert " << n << " vectors to cache";
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    Status status;
    zilliz::milvus::server::CollectInsertMetrics metrics(n, status);
    status = mem_mgr_->InsertVectors(table_id_, n, vectors, vector_ids_);
//    std::chrono::microseconds time_span = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
//    double average_time = double(time_span.count()) / n;

//    ENGINE_LOG_DEBUG << "Insert vectors to cache finished";

    return status;
}

Status DBImpl::CreateIndex(const std::string& table_id, const TableIndex& index) {
    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);

        //step 1: check index difference
        TableIndex old_index;
        auto status = DescribeIndex(table_id, old_index);
        if(!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to get table index info for table: " << table_id;
            return status;
        }

        //step 2: update index info
        TableIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;//dont change metric type, it was defined by CreateTable
        if(!utils::IsSameIndex(old_index, new_index)) {
            DropIndex(table_id);

            status = meta_ptr_->UpdateTableIndex(table_id, new_index);
            if (!status.ok()) {
                ENGINE_LOG_ERROR << "Failed to update table index info for table: " << table_id;
                return status;
            }
        }
    }

    //step 3: let merge file thread finish
    //to avoid duplicate data bug
    WaitMergeFileFinish();

    //step 4: wait and build index
    //for IDMAP type, only wait all NEW file converted to RAW file
    //for other type, wait NEW/RAW/NEW_MERGE/NEW_INDEX/TO_INDEX files converted to INDEX files
    std::vector<int> file_types;
    if(index.engine_type_ == (int)EngineType::FAISS_IDMAP) {
        file_types = {
                (int) meta::TableFileSchema::NEW,
                (int) meta::TableFileSchema::NEW_MERGE,
        };
    } else {
        file_types = {
                (int) meta::TableFileSchema::RAW,
                (int) meta::TableFileSchema::NEW,
                (int) meta::TableFileSchema::NEW_MERGE,
                (int) meta::TableFileSchema::NEW_INDEX,
                (int) meta::TableFileSchema::TO_INDEX,
        };
    }

    std::vector<std::string> file_ids;
    auto status = meta_ptr_->FilesByType(table_id, file_types, file_ids);
    int times = 1;

    while (!file_ids.empty()) {
        ENGINE_LOG_DEBUG << "Non index files detected! Will build index " << times;
        if(index.engine_type_ != (int)EngineType::FAISS_IDMAP) {
            status = meta_ptr_->UpdateTableFilesToIndex(table_id);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(10*1000, times*100)));
        status = meta_ptr_->FilesByType(table_id, file_types, file_ids);
        times++;
    }

    return Status::OK();
}

Status DBImpl::DescribeIndex(const std::string& table_id, TableIndex& index) {
    return meta_ptr_->DescribeTableIndex(table_id, index);
}

Status DBImpl::DropIndex(const std::string& table_id) {
    ENGINE_LOG_DEBUG << "Drop index for table: " << table_id;
    return meta_ptr_->DropTableIndex(table_id);
}

Status DBImpl::Query(const std::string &table_id, uint64_t k, uint64_t nq, uint64_t nprobe,
                      const float *vectors, QueryResults &results) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    meta::DatesT dates = {utils::GetDate()};
    Status result = Query(table_id, k, nq, nprobe, vectors, dates, results);

    return result;
}

Status DBImpl::Query(const std::string& table_id, uint64_t k, uint64_t nq, uint64_t nprobe,
        const float* vectors, const meta::DatesT& dates, QueryResults& results) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    ENGINE_LOG_DEBUG << "Query by dates for table: " << table_id;

    //get all table files from table
    meta::DatePartionedTableFilesSchema files;
    std::vector<size_t> ids;
    auto status = meta_ptr_->FilesToSearch(table_id, ids, dates, files);
    if (!status.ok()) { return status; }

    meta::TableFilesSchema file_id_array;
    for (auto &day_files : files) {
        for (auto &file : day_files.second) {
            file_id_array.push_back(file);
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info before query
    status = QueryAsync(table_id, file_id_array, k, nq, nprobe, vectors, dates, results);
    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info after query
    return status;
}

Status DBImpl::Query(const std::string& table_id, const std::vector<std::string>& file_ids,
        uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
        const meta::DatesT& dates, QueryResults& results) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    ENGINE_LOG_DEBUG << "Query by file ids for table: " << table_id;

    //get specified files
    std::vector<size_t> ids;
    for (auto &id : file_ids) {
        meta::TableFileSchema table_file;
        table_file.table_id_ = table_id;
        std::string::size_type sz;
        ids.push_back(std::stoul(id, &sz));
    }

    meta::DatePartionedTableFilesSchema files_array;
    auto status = meta_ptr_->FilesToSearch(table_id, ids, dates, files_array);
    if (!status.ok()) {
        return status;
    }

    meta::TableFilesSchema file_id_array;
    for (auto &day_files : files_array) {
        for (auto &file : day_files.second) {
            file_id_array.push_back(file);
        }
    }

    if(file_id_array.empty()) {
        return Status(DB_ERROR, "Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info before query
    status = QueryAsync(table_id, file_id_array, k, nq, nprobe, vectors, dates, results);
    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info after query
    return status;
}

Status DBImpl::Size(uint64_t& result) {
    if (shutting_down_.load(std::memory_order_acquire)){
        return Status(DB_ERROR, "Milsvus server is shutdown!");
    }

    return  meta_ptr_->Size(result);
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//internal methods
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status DBImpl::QueryAsync(const std::string& table_id, const meta::TableFilesSchema& files,
                          uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
                          const meta::DatesT& dates, QueryResults& results) {
    using namespace scheduler;
    server::CollectQueryMetrics metrics(nq);

    TimeRecorder rc("");

    //step 1: get files to search
    ENGINE_LOG_DEBUG << "Engine query begin, index file count: " << files.size() << " date range count: " << dates.size();
    SearchJobPtr job = std::make_shared<SearchJob>(0, k, nq, nprobe, vectors);
     for (auto &file : files) {
        TableFileSchemaPtr file_ptr = std::make_shared<meta::TableFileSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    //step 2: put search task to scheduler
    JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    //step 3: print time cost information
//    double load_cost = context->LoadCost();
//    double search_cost = context->SearchCost();
//    double reduce_cost = context->ReduceCost();
//    std::string load_info = TimeRecorder::GetTimeSpanStr(load_cost);
//    std::string search_info = TimeRecorder::GetTimeSpanStr(search_cost);
//    std::string reduce_info = TimeRecorder::GetTimeSpanStr(reduce_cost);
//    if(search_cost > 0.0 || reduce_cost > 0.0) {
//        double total_cost = load_cost + search_cost + reduce_cost;
//        double load_percent = load_cost/total_cost;
//        double search_percent = search_cost/total_cost;
//        double reduce_percent = reduce_cost/total_cost;
//
//        ENGINE_LOG_DEBUG << "Engine load index totally cost: " << load_info << " percent: " << load_percent*100 << "%";
//        ENGINE_LOG_DEBUG << "Engine search index totally cost: " << search_info << " percent: " << search_percent*100 << "%";
//        ENGINE_LOG_DEBUG << "Engine reduce topk totally cost: " << reduce_info << " percent: " << reduce_percent*100 << "%";
//    } else {
//        ENGINE_LOG_DEBUG << "Engine load cost: " << load_info
//            << " search cost: " << search_info
//            << " reduce cost: " << reduce_info;
//    }

    //step 4: construct results
    results = job->GetResult();
    rc.ElapseFromBegin("Engine query totally cost");

    return Status::OK();
}

void DBImpl::BackgroundTimerTask() {
    Status status;
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (shutting_down_.load(std::memory_order_acquire)){
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

void DBImpl::WaitMergeFileFinish() {
    std::lock_guard<std::mutex> lck(compact_result_mutex_);
    for(auto& iter : compact_thread_results_) {
        iter.wait();
    }
}

void DBImpl::WaitBuildIndexFinish() {
    std::lock_guard<std::mutex> lck(index_result_mutex_);
    for(auto& iter : index_thread_results_) {
        iter.wait();
    }
}

void DBImpl::StartMetricTask() {
    static uint64_t metric_clock_tick = 0;
    metric_clock_tick++;
    if(metric_clock_tick%METRIC_ACTION_INTERVAL != 0) {
        return;
    }

    ENGINE_LOG_TRACE << "Start metric task";

    server::Metrics::GetInstance().KeepingAliveCounterIncrement(METRIC_ACTION_INTERVAL);
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    server::Metrics::GetInstance().CpuCacheUsageGaugeSet(cache_usage*100/cache_total);
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

    ENGINE_LOG_TRACE << "Metric task finished";
}

Status DBImpl::MemSerialize() {
    std::lock_guard<std::mutex> lck(mem_serialize_mutex_);
    std::set<std::string> temp_table_ids;
    mem_mgr_->Serialize(temp_table_ids);
    for(auto& id : temp_table_ids) {
        compact_table_ids_.insert(id);
    }

    if(!temp_table_ids.empty()) {
        SERVER_LOG_DEBUG << "Insert cache serialized";
    }

    return Status::OK();
}

void DBImpl::StartCompactionTask() {
    static uint64_t compact_clock_tick = 0;
    compact_clock_tick++;
    if(compact_clock_tick%COMPACT_ACTION_INTERVAL != 0) {
        return;
    }

    //serialize memory data
    MemSerialize();

    //compactiong has been finished?
    {
        std::lock_guard<std::mutex> lck(compact_result_mutex_);
        if (!compact_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (compact_thread_results_.back().wait_for(span) == std::future_status::ready) {
                compact_thread_results_.pop_back();
            }
        }
    }

    //add new compaction task
    {
        std::lock_guard<std::mutex> lck(compact_result_mutex_);
        if (compact_thread_results_.empty()) {
            compact_thread_results_.push_back(
                compact_thread_pool_.enqueue(&DBImpl::BackgroundCompaction, this, compact_table_ids_));
            compact_table_ids_.clear();
        }
    }
}

Status DBImpl::MergeFiles(const std::string& table_id, const meta::DateT& date,
        const meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Merge files for table: " << table_id;

    //step 1: create table file
    meta::TableFileSchema table_file;
    table_file.table_id_ = table_id;
    table_file.date_ = date;
    table_file.file_type_ = meta::TableFileSchema::NEW_MERGE;
    Status status = meta_ptr_->CreateTableFile(table_file);

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to create table: " << status.ToString();
        return status;
    }

    //step 2: merge files
    ExecutionEnginePtr index =
            EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_,
                    (MetricType)table_file.metric_type_, table_file.nlist_);

    meta::TableFilesSchema updated;
    long  index_size = 0;

    for (auto& file : files) {
        server::CollectMergeFilesMetrics metrics;

        index->Merge(file.location_);
        auto file_schema = file;
        file_schema.file_type_ = meta::TableFileSchema::TO_DELETE;
        updated.push_back(file_schema);
        ENGINE_LOG_DEBUG << "Merging file " << file_schema.file_id_;
        index_size = index->Size();

        if (index_size >= file_schema.index_file_size_) break;
    }

    //step 3: serialize to disk
    try {
        index->Serialize();
    } catch (std::exception& ex) {
        //typical error: out of disk space or permition denied
        std::string msg = "Serialize merged index encounter exception: " + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;

        table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
        status = meta_ptr_->UpdateTableFile(table_file);
        ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

        std::cout << "ERROR: failed to persist merged index file: " << table_file.location_
                  << ", possible out of disk space" << std::endl;

        return Status(DB_ERROR, msg);
    }

    //step 4: update table files state
    //if index type isn't IDMAP, set file type to TO_INDEX if file size execeed index_file_size
    //else set file type to RAW, no need to build index
    if (table_file.engine_type_ != (int)EngineType::FAISS_IDMAP) {
        table_file.file_type_ = (index->PhysicalSize() >= table_file.index_file_size_) ?
                                meta::TableFileSchema::TO_INDEX : meta::TableFileSchema::RAW;
    } else {
        table_file.file_type_ = meta::TableFileSchema::RAW;
    }
    table_file.file_size_ = index->PhysicalSize();
    table_file.row_count_ = index->Count();
    updated.push_back(table_file);
    status = meta_ptr_->UpdateTableFiles(updated);
    ENGINE_LOG_DEBUG << "New merged file " << table_file.file_id_ <<
        " of size " << index->PhysicalSize() << " bytes";

    if(options_.insert_cache_immediately_) {
        index->Cache();
    }

    return status;
}

Status DBImpl::BackgroundMergeFiles(const std::string& table_id) {
    meta::DatePartionedTableFilesSchema raw_files;
    auto status = meta_ptr_->FilesToMerge(table_id, raw_files);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to get merge files for table: " << table_id;
        return status;
    }

    bool has_merge = false;
    for (auto& kv : raw_files) {
        auto files = kv.second;
        if (files.size() < options_.merge_trigger_number_) {
            ENGINE_LOG_DEBUG << "Files number not greater equal than merge trigger number, skip merge action";
            continue;
        }
        has_merge = true;
        MergeFiles(table_id, kv.first, kv.second);

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action for table: " << table_id;
            break;
        }
    }

    return Status::OK();
}

void DBImpl::BackgroundCompaction(std::set<std::string> table_ids) {
    ENGINE_LOG_TRACE << " Background compaction thread start";

    Status status;
    for (auto& table_id : table_ids) {
        status = BackgroundMergeFiles(table_id);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Merge files for table " << table_id << " failed: " << status.ToString();
        }

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action";
            break;
        }
    }

    meta_ptr_->Archive();

    int ttl = 5*meta::M_SEC;//default: file will be deleted after 5 minutes
    if (options_.mode_ == DBOptions::MODE::CLUSTER) {
        ttl = meta::D_SEC;
    }
    meta_ptr_->CleanUpFilesWithTTL(ttl);

    ENGINE_LOG_TRACE << " Background compaction thread exit";
}

void DBImpl::StartBuildIndexTask(bool force) {
    static uint64_t index_clock_tick = 0;
    index_clock_tick++;
    if(!force && (index_clock_tick%INDEX_ACTION_INTERVAL != 0)) {
        return;
    }

    //build index has been finished?
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (!index_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (index_thread_results_.back().wait_for(span) == std::future_status::ready) {
                index_thread_results_.pop_back();
            }
        }
    }

    //add new build index task
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (index_thread_results_.empty()) {
            index_thread_results_.push_back(
                index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndex, this));
        }
    }
}

Status DBImpl::BuildIndex(const meta::TableFileSchema& file) {
    ExecutionEnginePtr to_index =
            EngineFactory::Build(file.dimension_, file.location_, (EngineType)file.engine_type_,
                    (MetricType)file.metric_type_, file.nlist_);
    if(to_index == nullptr) {
        ENGINE_LOG_ERROR << "Invalid engine type";
        return Status(DB_ERROR, "Invalid engine type");
    }

    try {
        //step 1: load index
        Status status = to_index->Load(options_.insert_cache_immediately_);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to load index file: " << status.ToString();
            return status;
        }

        //step 2: create table file
        meta::TableFileSchema table_file;
        table_file.table_id_ = file.table_id_;
        table_file.date_ = file.date_;
        table_file.file_type_ = meta::TableFileSchema::NEW_INDEX; //for multi-db-path, distribute index file averagely to each path
        status = meta_ptr_->CreateTableFile(table_file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to create table file: " << status.ToString();
            return status;
        }

        //step 3: build index
        std::shared_ptr<ExecutionEngine> index;

        try {
            server::CollectBuildIndexMetrics metrics;
            index = to_index->BuildIndex(table_file.location_, (EngineType)table_file.engine_type_);
            if (index == nullptr) {
                table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
                status = meta_ptr_->UpdateTableFile(table_file);
                ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

                return status;
            }

        } catch (std::exception& ex) {
            //typical error: out of gpu memory
            std::string msg = "BuildIndex encounter exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;

            table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
            status = meta_ptr_->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

            std::cout << "ERROR: failed to build index, index file is too large or gpu memory is not enough" << std::endl;

            return Status(DB_ERROR, msg);
        }

        //step 4: if table has been deleted, dont save index file
        bool has_table = false;
        meta_ptr_->HasTable(file.table_id_, has_table);
        if(!has_table) {
            meta_ptr_->DeleteTableFiles(file.table_id_);
            return Status::OK();
        }

        //step 5: save index file
        try {
            index->Serialize();
        } catch (std::exception& ex) {
            //typical error: out of disk space or permition denied
            std::string msg = "Serialize index encounter exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;

            table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
            status = meta_ptr_->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

            std::cout << "ERROR: failed to persist index file: " << table_file.location_
                << ", possible out of disk space" << std::endl;

            return Status(DB_ERROR, msg);
        }

        //step 6: update meta
        table_file.file_type_ = meta::TableFileSchema::INDEX;
        table_file.file_size_ = index->PhysicalSize();
        table_file.row_count_ = index->Count();

        auto origin_file = file;
        origin_file.file_type_ = meta::TableFileSchema::BACKUP;

        meta::TableFilesSchema update_files = {table_file, origin_file};
        status = meta_ptr_->UpdateTableFiles(update_files);
        if(status.ok()) {
            ENGINE_LOG_DEBUG << "New index file " << table_file.file_id_ << " of size "
                             << index->PhysicalSize() << " bytes"
                             << " from file " << origin_file.file_id_;

            if(options_.insert_cache_immediately_) {
                index->Cache();
            }
        } else {
            //failed to update meta, mark the new file as to_delete, don't delete old file
            origin_file.file_type_ = meta::TableFileSchema::TO_INDEX;
            status = meta_ptr_->UpdateTableFile(origin_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << origin_file.file_id_ << " to to_index";

            table_file.file_type_ = meta::TableFileSchema::TO_DELETE;
            status = meta_ptr_->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";
        }

    } catch (std::exception& ex) {
        std::string msg = "Build index encounter exception: " + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

void DBImpl::BackgroundBuildIndex() {
    ENGINE_LOG_TRACE << "Background build index thread start";

    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::TableFilesSchema to_index_files;
    meta_ptr_->FilesToIndex(to_index_files);
    Status status;
    for (auto& file : to_index_files) {
        status = BuildIndex(file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Building index for " << file.id_ << " failed: " << status.ToString();
        }

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip build index action";
            break;
        }
    }

    ENGINE_LOG_TRACE << "Background build index thread exit";
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
