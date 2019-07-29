/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Log.h"
#include "EngineFactory.h"
#include "Factories.h"
#include "metrics/Metrics.h"
#include "scheduler/TaskScheduler.h"

#include "scheduler/context/DeleteContext.h"
#include "utils/TimeRecorder.h"
#include "MetaConsts.h"

#include <assert.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <cache/CpuCacheMgr.h>
#include <boost/filesystem.hpp>

namespace zilliz {
namespace milvus {
namespace engine {

namespace {

constexpr uint64_t METRIC_ACTION_INTERVAL = 1;
constexpr uint64_t COMPACT_ACTION_INTERVAL = 1;
constexpr uint64_t INDEX_ACTION_INTERVAL = 1;

void CollectInsertMetrics(double total_time, size_t n, bool succeed) {
    double avg_time = total_time / n;
    for (int i = 0; i < n; ++i) {
        server::Metrics::GetInstance().AddVectorsDurationHistogramOberve(avg_time);
    }

//    server::Metrics::GetInstance().add_vector_duration_seconds_quantiles().Observe((average_time));
    if (succeed) {
        server::Metrics::GetInstance().AddVectorsSuccessTotalIncrement(n);
        server::Metrics::GetInstance().AddVectorsSuccessGaugeSet(n);
    }
    else {
        server::Metrics::GetInstance().AddVectorsFailTotalIncrement(n);
        server::Metrics::GetInstance().AddVectorsFailGaugeSet(n);
    }
}

void CollectQueryMetrics(double total_time, size_t nq) {
    for (int i = 0; i < nq; ++i) {
        server::Metrics::GetInstance().QueryResponseSummaryObserve(total_time);
    }
    auto average_time = total_time / nq;
    server::Metrics::GetInstance().QueryVectorResponseSummaryObserve(average_time, nq);
    server::Metrics::GetInstance().QueryVectorResponsePerSecondGaugeSet(double (nq) / total_time);
}

void CollectFileMetrics(int file_type, size_t file_size, double total_time) {
    switch(file_type) {
        case meta::TableFileSchema::RAW:
        case meta::TableFileSchema::TO_INDEX: {
            server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
            server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size);
            break;
        }
        default: {
            server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
            server::Metrics::GetInstance().IndexFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().IndexFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().IndexFileSizeGaugeSet(file_size);
            break;
        }
    }
}
}


DBImpl::DBImpl(const Options& options)
    : options_(options),
      shutting_down_(false),
      compact_thread_pool_(1, 1),
      index_thread_pool_(1, 1) {
    meta_ptr_ = DBMetaImplFactory::Build(options.meta, options.mode);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);
    if (options.mode != Options::MODE::READ_ONLY) {
        ENGINE_LOG_TRACE << "StartTimerTasks";
        StartTimerTasks();
    }


}

Status DBImpl::CreateTable(meta::TableSchema& table_schema) {
    return meta_ptr_->CreateTable(table_schema);
}

Status DBImpl::DeleteTable(const std::string& table_id, const meta::DatesT& dates) {
    //dates partly delete files of the table but currently we don't support
    ENGINE_LOG_DEBUG << "Prepare to delete table " << table_id;

    mem_mgr_->EraseMemVector(table_id); //not allow insert
    meta_ptr_->DeleteTable(table_id); //soft delete table

    //scheduler will determine when to delete table files
    TaskScheduler& scheduler = TaskScheduler::GetInstance();
    DeleteContextPtr context = std::make_shared<DeleteContext>(table_id, meta_ptr_);
    scheduler.Schedule(context);

    return Status::OK();
}

Status DBImpl::DescribeTable(meta::TableSchema& table_schema) {
    return meta_ptr_->DescribeTable(table_schema);
}

Status DBImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    return meta_ptr_->HasTable(table_id, has_or_not);
}

Status DBImpl::AllTables(std::vector<meta::TableSchema>& table_schema_array) {
    return meta_ptr_->AllTables(table_schema_array);
}

Status DBImpl::GetTableRowCount(const std::string& table_id, uint64_t& row_count) {
    return meta_ptr_->Count(table_id, row_count);
}

Status DBImpl::InsertVectors(const std::string& table_id_,
        uint64_t n, const float* vectors, IDNumbers& vector_ids_) {
    ENGINE_LOG_DEBUG << "Insert " << n << " vectors to cache";

    auto start_time = METRICS_NOW_TIME;
    Status status = mem_mgr_->InsertVectors(table_id_, n, vectors, vector_ids_);
    auto end_time = METRICS_NOW_TIME;
    double total_time = METRICS_MICROSECONDS(start_time,end_time);
//    std::chrono::microseconds time_span = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
//    double average_time = double(time_span.count()) / n;

    ENGINE_LOG_DEBUG << "Insert vectors to cache finished";

    CollectInsertMetrics(total_time, n, status.ok());
    return status;

}

Status DBImpl::Query(const std::string &table_id, uint64_t k, uint64_t nq,
                      const float *vectors, QueryResults &results) {
    auto start_time = METRICS_NOW_TIME;
    meta::DatesT dates = {meta::Meta::GetDate()};
    Status result = Query(table_id, k, nq, vectors, dates, results);
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time,end_time);

    CollectQueryMetrics(total_time, nq);

    return result;
}

Status DBImpl::Query(const std::string& table_id, uint64_t k, uint64_t nq,
        const float* vectors, const meta::DatesT& dates, QueryResults& results) {
    ENGINE_LOG_DEBUG << "Query by vectors";

    //get all table files from table
    meta::DatePartionedTableFilesSchema files;
    auto status = meta_ptr_->FilesToSearch(table_id, dates, files);
    if (!status.ok()) { return status; }

    meta::TableFilesSchema file_id_array;
    for (auto &day_files : files) {
        for (auto &file : day_files.second) {
            file_id_array.push_back(file);
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info before query
    status = QueryAsync(table_id, file_id_array, k, nq, vectors, dates, results);
    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info after query
    return status;
}

Status DBImpl::Query(const std::string& table_id, const std::vector<std::string>& file_ids,
        uint64_t k, uint64_t nq, const float* vectors,
        const meta::DatesT& dates, QueryResults& results) {
    ENGINE_LOG_DEBUG << "Query by file ids";

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
        return Status::Error("Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info before query
    status = QueryAsync(table_id, file_id_array, k, nq, vectors, dates, results);
    cache::CpuCacheMgr::GetInstance()->PrintInfo(); //print cache info after query
    return status;
}

Status DBImpl::QueryAsync(const std::string& table_id, const meta::TableFilesSchema& files,
                          uint64_t k, uint64_t nq, const float* vectors,
                          const meta::DatesT& dates, QueryResults& results) {
    auto start_time = METRICS_NOW_TIME;
    server::TimeRecorder rc("");

    //step 1: get files to search
    ENGINE_LOG_DEBUG << "Engine query begin, index file count:" << files.size() << " date range count:" << dates.size();
    SearchContextPtr context = std::make_shared<SearchContext>(k, nq, vectors);
    for (auto &file : files) {
        TableFileSchemaPtr file_ptr = std::make_shared<meta::TableFileSchema>(file);
        context->AddIndexFile(file_ptr);
    }

    //step 2: put search task to scheduler
    TaskScheduler& scheduler = TaskScheduler::GetInstance();
    scheduler.Schedule(context);

    context->WaitResult();

    //step 3: print time cost information
    double load_cost = context->LoadCost();
    double search_cost = context->SearchCost();
    double reduce_cost = context->ReduceCost();
    std::string load_info = server::TimeRecorder::GetTimeSpanStr(load_cost);
    std::string search_info = server::TimeRecorder::GetTimeSpanStr(search_cost);
    std::string reduce_info = server::TimeRecorder::GetTimeSpanStr(reduce_cost);
    if(search_cost > 0.0 || reduce_cost > 0.0) {
        double total_cost = load_cost + search_cost + reduce_cost;
        double load_percent = load_cost/total_cost;
        double search_percent = search_cost/total_cost;
        double reduce_percent = reduce_cost/total_cost;

        ENGINE_LOG_DEBUG << "Engine load index totally cost:" << load_info << " percent: " << load_percent*100 << "%";
        ENGINE_LOG_DEBUG << "Engine search index totally cost:" << search_info << " percent: " << search_percent*100 << "%";
        ENGINE_LOG_DEBUG << "Engine reduce topk totally cost:" << reduce_info << " percent: " << reduce_percent*100 << "%";
    } else {
        ENGINE_LOG_DEBUG << "Engine load cost:" << load_info
            << " search cost: " << search_info
            << " reduce cost: " << reduce_info;
    }

    //step 4: construct results
    results = context->GetResult();
    rc.ElapseFromBegin("Engine query totally cost");

    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time,end_time);

    CollectQueryMetrics(total_time, nq);

    return Status::OK();
}

void DBImpl::StartTimerTasks() {
    bg_timer_thread_ = std::thread(&DBImpl::BackgroundTimerTask, this);
}

void DBImpl::BackgroundTimerTask() {
    Status status;
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (shutting_down_.load(std::memory_order_acquire)){
            for(auto& iter : compact_thread_results_) {
                iter.wait();
            }
            for(auto& iter : index_thread_results_) {
                iter.wait();
            }

            ENGINE_LOG_DEBUG << "DB background thread exit";
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        StartMetricTask();
        StartCompactionTask();
        StartBuildIndexTask();
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
    server::Metrics::GetInstance().CacheUsageGaugeSet(cache_usage*100/cache_total);
    uint64_t size;
    Size(size);
    server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
    server::Metrics::GetInstance().CPUUsagePercentSet();
    server::Metrics::GetInstance().RAMUsagePercentSet();
    server::Metrics::GetInstance().GPUPercentGaugeSet();
    server::Metrics::GetInstance().GPUMemoryUsageGaugeSet();
    server::Metrics::GetInstance().OctetsSet();

    ENGINE_LOG_TRACE << "Metric task finished";
}

void DBImpl::StartCompactionTask() {
    static uint64_t compact_clock_tick = 0;
    compact_clock_tick++;
    if(compact_clock_tick%COMPACT_ACTION_INTERVAL != 0) {
        return;
    }

    //serialize memory data
    std::set<std::string> temp_table_ids;
    mem_mgr_->Serialize(temp_table_ids);
    for(auto& id : temp_table_ids) {
        compact_table_ids_.insert(id);
    }

    if(!temp_table_ids.empty()) {
        SERVER_LOG_DEBUG << "Insert cache serialized";
    }

    //compactiong has been finished?
    if(!compact_thread_results_.empty()) {
        std::chrono::milliseconds span(10);
        if (compact_thread_results_.back().wait_for(span) == std::future_status::ready) {
            compact_thread_results_.pop_back();
        }
    }

    //add new compaction task
    if(compact_thread_results_.empty()) {
        compact_thread_results_.push_back(
                compact_thread_pool_.enqueue(&DBImpl::BackgroundCompaction, this, compact_table_ids_));
        compact_table_ids_.clear();
    }
}

Status DBImpl::MergeFiles(const std::string& table_id, const meta::DateT& date,
        const meta::TableFilesSchema& files) {
    ENGINE_LOG_DEBUG << "Merge files for table " << table_id;

    meta::TableFileSchema table_file;
    table_file.table_id_ = table_id;
    table_file.date_ = date;
    Status status = meta_ptr_->CreateTableFile(table_file);

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Failed to create table: " << status.ToString();
        return status;
    }

    ExecutionEnginePtr index =
            EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_);

    meta::TableFilesSchema updated;
    long  index_size = 0;

    for (auto& file : files) {

        auto start_time = METRICS_NOW_TIME;
        index->Merge(file.location_);
        auto file_schema = file;
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time,end_time);
        server::Metrics::GetInstance().MemTableMergeDurationSecondsHistogramObserve(total_time);

        file_schema.file_type_ = meta::TableFileSchema::TO_DELETE;
        updated.push_back(file_schema);
        ENGINE_LOG_DEBUG << "Merging file " << file_schema.file_id_;
        index_size = index->Size();

        if (index_size >= options_.index_trigger_size) break;
    }


    index->Serialize();

    if (index_size >= options_.index_trigger_size) {
        table_file.file_type_ = meta::TableFileSchema::TO_INDEX;
    } else {
        table_file.file_type_ = meta::TableFileSchema::RAW;
    }
    table_file.size_ = index_size;
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
        if (files.size() < options_.merge_trigger_number) {
            ENGINE_LOG_DEBUG << "Files number not greater equal than merge trigger number, skip merge action";
            continue;
        }
        has_merge = true;
        MergeFiles(table_id, kv.first, kv.second);

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action for table " << table_id;
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
            continue;//let other table get chance to merge
        }

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip merge action";
            break;
        }
    }

    meta_ptr_->Archive();

    int ttl = 1;
    if (options_.mode == Options::MODE::CLUSTER) {
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
    if(!index_thread_results_.empty()) {
        std::chrono::milliseconds span(10);
        if (index_thread_results_.back().wait_for(span) == std::future_status::ready) {
            index_thread_results_.pop_back();
        }
    }

    //add new build index task
    if(index_thread_results_.empty()) {
        index_thread_results_.push_back(
                index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndex, this));
    }
}

Status DBImpl::BuildIndex(const std::string& table_id) {
    bool has = false;
    meta_ptr_->HasNonIndexFiles(table_id, has);
    int times = 1;

    while (has) {
        ENGINE_LOG_DEBUG << "Non index files detected! Will build index " << times;
        meta_ptr_->UpdateTableFilesToIndex(table_id);
        /* StartBuildIndexTask(true); */
        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(10*1000, times*100)));
        meta_ptr_->HasNonIndexFiles(table_id, has);
        times++;
    }
    return Status::OK();
    /* return BuildIndexByTable(table_id); */
}

Status DBImpl::BuildIndex(const meta::TableFileSchema& file) {
    ExecutionEnginePtr to_index = EngineFactory::Build(file.dimension_, file.location_, (EngineType)file.engine_type_);
    if(to_index == nullptr) {
        ENGINE_LOG_ERROR << "Invalid engine type";
        return Status::Error("Invalid engine type");
    }

    try {
        //step 1: load index
        to_index->Load(options_.insert_cache_immediately_);

        //step 2: create table file
        meta::TableFileSchema table_file;
        table_file.table_id_ = file.table_id_;
        table_file.date_ = file.date_;
        table_file.file_type_ = meta::TableFileSchema::INDEX; //for multi-db-path, distribute index file averagely to each path
        Status status = meta_ptr_->CreateTableFile(table_file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to create table: " << status.ToString();
            return status;
        }

        //step 3: build index
        auto start_time = METRICS_NOW_TIME;
        auto index = to_index->BuildIndex(table_file.location_);
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time, end_time);
        server::Metrics::GetInstance().BuildIndexDurationSecondsHistogramObserve(total_time);

        //step 4: if table has been deleted, dont save index file
        bool has_table = false;
        meta_ptr_->HasTable(file.table_id_, has_table);
        if(!has_table) {
            meta_ptr_->DeleteTableFiles(file.table_id_);
            return Status::OK();
        }

        //step 5: save index file
        index->Serialize();

        //step 6: update meta
        table_file.file_type_ = meta::TableFileSchema::INDEX;
        table_file.size_ = index->Size();

        auto to_remove = file;
        to_remove.file_type_ = meta::TableFileSchema::TO_DELETE;

        meta::TableFilesSchema update_files = {to_remove, table_file};
        meta_ptr_->UpdateTableFiles(update_files);

        ENGINE_LOG_DEBUG << "New index file " << table_file.file_id_ << " of size "
                   << index->PhysicalSize() << " bytes"
                   << " from file " << to_remove.file_id_;

        if(options_.insert_cache_immediately_) {
            index->Cache();
        }

    } catch (std::exception& ex) {
        std::string msg = "Build index encounter exception" + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;
        return Status::Error(msg);
    }

    return Status::OK();
}

Status DBImpl::BuildIndexByTable(const std::string& table_id) {
    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::TableFilesSchema to_index_files;
    meta_ptr_->FilesToIndex(to_index_files);

    Status status;

    for (auto& file : to_index_files) {
        status = BuildIndex(file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Building index for " << file.id_ << " failed: " << status.ToString();
            return status;
        }
        ENGINE_LOG_DEBUG << "Sync building index for " << file.id_ << " passed";

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip build index action for table " << table_id;
            break;
        }
    }

    return status;
}

void DBImpl::BackgroundBuildIndex() {
    ENGINE_LOG_TRACE << " Background build index thread start";

    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::TableFilesSchema to_index_files;
    meta_ptr_->FilesToIndex(to_index_files);
    Status status;
    for (auto& file : to_index_files) {
        status = BuildIndex(file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Building index for " << file.id_ << " failed: " << status.ToString();
            return;
        }

        if (shutting_down_.load(std::memory_order_acquire)){
            ENGINE_LOG_DEBUG << "Server will shutdown, skip build index action";
            break;
        }
    }

    ENGINE_LOG_TRACE << " Background build index thread exit";
}

Status DBImpl::DropAll() {
    return meta_ptr_->DropAll();
}

Status DBImpl::Size(uint64_t& result) {
    return  meta_ptr_->Size(result);
}

DBImpl::~DBImpl() {
    shutting_down_.store(true, std::memory_order_release);
    bg_timer_thread_.join();
    std::set<std::string> ids;
    mem_mgr_->Serialize(ids);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
