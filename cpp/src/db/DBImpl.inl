/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Env.h"

#include <assert.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <easylogging++.h>
#include <cache/CpuCacheMgr.h>
#include "../utils/Log.h"
#include "metrics/Metrics.h"

namespace zilliz {
namespace vecwise {
namespace engine {


template<typename EngineT>
DBImpl<EngineT>::DBImpl(const Options& options)
    : env_(options.env),
      options_(options),
      bg_compaction_scheduled_(false),
      shutting_down_(false),
      bg_build_index_started_(false),
      pMeta_(new meta::DBMetaImpl(options_.meta)),
      pMemMgr_(new MemManager<EngineT>(pMeta_, options_)) {
    StartTimerTasks(options_.memory_sync_interval);
}

template<typename EngineT>
Status DBImpl<EngineT>::CreateTable(meta::TableSchema& table_schema) {
    return pMeta_->CreateTable(table_schema);
}

template<typename EngineT>
Status DBImpl<EngineT>::DescribeTable(meta::TableSchema& table_schema) {
    return pMeta_->DescribeTable(table_schema);
}

template<typename EngineT>
Status DBImpl<EngineT>::HasTable(const std::string& table_id, bool& has_or_not) {
    return pMeta_->HasTable(table_id, has_or_not);
}

template<typename EngineT>
Status DBImpl<EngineT>::InsertVectors(const std::string& table_id_,
        size_t n, const float* vectors, IDNumbers& vector_ids_) {

    auto start_time = METRICS_NOW_TIME;
    Status status = pMemMgr_->InsertVectors(table_id_, n, vectors, vector_ids_);
    auto end_time = METRICS_NOW_TIME;

//    std::chrono::microseconds time_span = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
//    double average_time = double(time_span.count()) / n;

    double total_time = METRICS_MICROSECONDS(start_time,end_time);
    double avg_time = total_time / double(n);
    for (int i = 0; i < n; ++i) {
        server::Metrics::GetInstance().AddVectorsDurationHistogramOberve(avg_time);
    }

//    server::Metrics::GetInstance().add_vector_duration_seconds_quantiles().Observe((average_time));
    if (!status.ok()) {
        server::Metrics::GetInstance().AddVectorsFailTotalIncrement(n);
        server::Metrics::GetInstance().AddVectorsFailGaugeSet(n);
        return status;
    }
    server::Metrics::GetInstance().AddVectorsSuccessTotalIncrement(n);
    server::Metrics::GetInstance().AddVectorsSuccessGaugeSet(n);
}

template<typename EngineT>
Status DBImpl<EngineT>::Query(const std::string &table_id, size_t k, size_t nq,
                      const float *vectors, QueryResults &results) {
    auto start_time = METRICS_NOW_TIME;
    meta::DatesT dates = {meta::Meta::GetDate()};
    Status result = Query(table_id, k, nq, vectors, dates, results);
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    auto average_time = total_time / nq;
    for (int i = 0; i < nq; ++i) {
        server::Metrics::GetInstance().QueryResponseSummaryObserve(total_time);
    }
    server::Metrics::GetInstance().QueryVectorResponseSummaryObserve(average_time, nq);
    server::Metrics::GetInstance().QueryVectorResponsePerSecondGaugeSet(double (nq) / total_time);
    server::Metrics::GetInstance().QueryResponsePerSecondGaugeSet(1.0 / total_time);
    return result;
}

template<typename EngineT>
Status DBImpl<EngineT>::Query(const std::string& table_id, size_t k, size_t nq,
        const float* vectors, const meta::DatesT& dates, QueryResults& results) {

    meta::DatePartionedTableFilesSchema files;
    auto status = pMeta_->FilesToSearch(table_id, dates, files);
    if (!status.ok()) { return status; }

    LOG(DEBUG) << "Search DateT Size=" << files.size();

    meta::TableFilesSchema index_files;
    meta::TableFilesSchema raw_files;
    for (auto &day_files : files) {
        for (auto &file : day_files.second) {
            file.file_type == meta::TableFileSchema::INDEX ?
            index_files.push_back(file) : raw_files.push_back(file);
        }
    }

    int dim = 0;
    if (!index_files.empty()) {
        dim = index_files[0].dimension;
    } else if (!raw_files.empty()) {
        dim = raw_files[0].dimension;
    } else {
        LOG(DEBUG) << "no files to search";
        return Status::OK();
    }

    {
        // [{ids, distence}, ...]
        using SearchResult = std::pair<std::vector<long>, std::vector<float>>;
        std::vector<SearchResult> batchresult(nq); // allocate nq cells.

        auto cluster = [&](long *nns, float *dis, const int& k) -> void {
            for (int i = 0; i < nq; ++i) {
                auto f_begin = batchresult[i].first.cbegin();
                auto s_begin = batchresult[i].second.cbegin();
                batchresult[i].first.insert(f_begin, nns + i * k, nns + i * k + k);
                batchresult[i].second.insert(s_begin, dis + i * k, dis + i * k + k);
            }
        };

        // Allocate Memory
        float *output_distence;
        long *output_ids;
        output_distence = (float *) malloc(k * nq * sizeof(float));
        output_ids = (long *) malloc(k * nq * sizeof(long));
        memset(output_distence, 0, k * nq * sizeof(float));
        memset(output_ids, 0, k * nq * sizeof(long));

        long search_set_size = 0;

        auto search_in_index = [&](meta::TableFilesSchema& file_vec) -> void {
            for (auto &file : file_vec) {
                EngineT index(file.dimension, file.location);
                index.Load();
                auto file_size = index.PhysicalSize()/(1024*1024);
                search_set_size += file_size;

                LOG(DEBUG) << "Search file_type " << file.file_type << " Of Size: "
                    << file_size << " M";

                int inner_k = index.Count() < k ? index.Count() : k;
                auto start_time = METRICS_NOW_TIME;
                index.Search(nq, vectors, inner_k, output_distence, output_ids);
                auto end_time = METRICS_NOW_TIME;
                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
                if(file.file_type == meta::TableFileSchema::RAW) {
                    server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                    server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size*1024*1024);
                    server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size*1024*1024);
                    server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size*1024*1024);

                } else if(file.file_type == meta::TableFileSchema::TO_INDEX) {

                    server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                    server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size*1024*1024);
                    server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size*1024*1024);
                    server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size*1024*1024);

                } else {
                    server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
                    server::Metrics::GetInstance().IndexFileSizeHistogramObserve(file_size*1024*1024);
                    server::Metrics::GetInstance().IndexFileSizeTotalIncrement(file_size*1024*1024);
                    server::Metrics::GetInstance().IndexFileSizeGaugeSet(file_size*1024*1024);
                }
                cluster(output_ids, output_distence, inner_k); // cluster to each query
                memset(output_distence, 0, k * nq * sizeof(float));
                memset(output_ids, 0, k * nq * sizeof(long));
            }
        };

        auto topk_cpu = [](const std::vector<float> &input_data,
                           const int &k,
                           float *output_distence,
                           long *output_ids) -> void {
            std::map<float, std::vector<int>> inverted_table;
            for (int i = 0; i < input_data.size(); ++i) {
                if (inverted_table.count(input_data[i]) == 1) {
                    auto& ori_vec = inverted_table[input_data[i]];
                    ori_vec.push_back(i);
                }
                else {
                    inverted_table[input_data[i]] = std::vector<int>{i};
                }
            }

            int count = 0;
            for (auto &item : inverted_table){
                if (count == k) break;
                for (auto &id : item.second){
                    output_distence[count] = item.first;
                    output_ids[count] = id;
                    if (++count == k) break;
                }
            }
        };
        auto cluster_topk = [&]() -> void {
            QueryResult res;
            for (auto &result_pair : batchresult) {
                auto &dis = result_pair.second;
                auto &nns = result_pair.first;

                topk_cpu(dis, k, output_distence, output_ids);

                int inner_k = dis.size() < k ? dis.size() : k;
                for (int i = 0; i < inner_k; ++i) {
                    res.emplace_back(nns[output_ids[i]]); // mapping
                }
                results.push_back(res); // append to result list
                res.clear();
                memset(output_distence, 0, k * nq * sizeof(float));
                memset(output_ids, 0, k * nq * sizeof(long));
            }
        };

        search_in_index(raw_files);
        search_in_index(index_files);

        LOG(DEBUG) << "Search Overall Set Size=" << search_set_size << " M";
        cluster_topk();

        free(output_distence);
        free(output_ids);
    }

    if (results.empty()) {
        return Status::NotFound("Group " + table_id + ", search result not found!");
    }
    return Status::OK();
}

template<typename EngineT>
void DBImpl<EngineT>::StartTimerTasks(int interval) {
    bg_timer_thread_ = std::thread(&DBImpl<EngineT>::BackgroundTimerTask, this, interval);
}

template<typename EngineT>
void DBImpl<EngineT>::BackgroundTimerTask(int interval) {
    Status status;
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!bg_error_.ok()) break;
        if (shutting_down_.load(std::memory_order_acquire)) break;

        std::this_thread::sleep_for(std::chrono::seconds(interval));
        server::Metrics::GetInstance().KeepingAliveCounterIncrement(interval);
        int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
        int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
        server::Metrics::GetInstance().CacheUsageGaugeSet(cache_usage*100/cache_total);
        long size;
        Size(size);
        server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
        server::Metrics::GetInstance().CPUUsagePercentSet();
        server::Metrics::GetInstance().RAMUsagePercentSet();
        server::Metrics::GetInstance().GPUPercentGaugeSet();
        server::Metrics::GetInstance().GPUMemoryUsageGaugeSet();
        TrySchedule();
    }
}

template<typename EngineT>
void DBImpl<EngineT>::TrySchedule() {
    if (bg_compaction_scheduled_) return;
    if (!bg_error_.ok()) return;

    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl<EngineT>::BGWork, this);
}

template<typename EngineT>
void DBImpl<EngineT>::BGWork(void* db_) {
    reinterpret_cast<DBImpl*>(db_)->BackgroundCall();
}

template<typename EngineT>
void DBImpl<EngineT>::BackgroundCall() {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(bg_compaction_scheduled_);

    if (!bg_error_.ok() || shutting_down_.load(std::memory_order_acquire))
        return ;

    BackgroundCompaction();

    bg_compaction_scheduled_ = false;
    bg_work_finish_signal_.notify_all();
}


template<typename EngineT>
Status DBImpl<EngineT>::MergeFiles(const std::string& table_id, const meta::DateT& date,
        const meta::TableFilesSchema& files) {
    meta::TableFileSchema table_file;
    table_file.table_id = table_id;
    table_file.date = date;
    Status status = pMeta_->CreateTableFile(table_file);

    if (!status.ok()) {
        LOG(INFO) << status.ToString() << std::endl;
        return status;
    }

    EngineT index(table_file.dimension, table_file.location);

    meta::TableFilesSchema updated;
    long  index_size = 0;

    for (auto& file : files) {

        auto start_time = METRICS_NOW_TIME;
        index.Merge(file.location);
        auto file_schema = file;
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time,end_time);
        server::Metrics::GetInstance().MemTableMergeDurationSecondsHistogramObserve(total_time);

        file_schema.file_type = meta::TableFileSchema::TO_DELETE;
        updated.push_back(file_schema);
        LOG(DEBUG) << "Merging file " << file_schema.file_id;
        index_size = index.Size();

        if (index_size >= options_.index_trigger_size) break;
    }


    index.Serialize();

    if (index_size >= options_.index_trigger_size) {
        table_file.file_type = meta::TableFileSchema::TO_INDEX;
    } else {
        table_file.file_type = meta::TableFileSchema::RAW;
    }
    table_file.size = index_size;
    updated.push_back(table_file);
    status = pMeta_->UpdateTableFiles(updated);
    LOG(DEBUG) << "New merged file " << table_file.file_id <<
        " of size=" << index.PhysicalSize()/(1024*1024) << " M";

    index.Cache();

    return status;
}

template<typename EngineT>
Status DBImpl<EngineT>::BackgroundMergeFiles(const std::string& table_id) {
    meta::DatePartionedTableFilesSchema raw_files;
    auto status = pMeta_->FilesToMerge(table_id, raw_files);
    if (!status.ok()) {
        return status;
    }

    bool has_merge = false;

    for (auto& kv : raw_files) {
        auto files = kv.second;
        if (files.size() <= options_.merge_trigger_number) {
            continue;
        }
        has_merge = true;
        MergeFiles(table_id, kv.first, kv.second);
    }

    pMeta_->Archive();

    TryBuildIndex();

    pMeta_->CleanUpFilesWithTTL(1);

    return Status::OK();
}

template<typename EngineT>
Status DBImpl<EngineT>::BuildIndex(const meta::TableFileSchema& file) {
    meta::TableFileSchema table_file;
    table_file.table_id = file.table_id;
    table_file.date = file.date;
    Status status = pMeta_->CreateTableFile(table_file);
    if (!status.ok()) {
        return status;
    }

    EngineT to_index(file.dimension, file.location);

    to_index.Load();
    auto start_time = METRICS_NOW_TIME;
    auto index = to_index.BuildIndex(table_file.location);
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    server::Metrics::GetInstance().BuildIndexDurationSecondsHistogramObserve(total_time);

    table_file.file_type = meta::TableFileSchema::INDEX;
    table_file.size = index->Size();

    auto to_remove = file;
    to_remove.file_type = meta::TableFileSchema::TO_DELETE;

    meta::TableFilesSchema update_files = {to_remove, table_file};
    pMeta_->UpdateTableFiles(update_files);

    LOG(DEBUG) << "New index file " << table_file.file_id << " of size "
        << index->PhysicalSize()/(1024*1024) << " M"
        << " from file " << to_remove.file_id;

    index->Cache();
    pMeta_->Archive();

    return Status::OK();
}

template<typename EngineT>
void DBImpl<EngineT>::BackgroundBuildIndex() {
    std::lock_guard<std::mutex> lock(build_index_mutex_);
    assert(bg_build_index_started_);
    meta::TableFilesSchema to_index_files;
    pMeta_->FilesToIndex(to_index_files);
    Status status;
    for (auto& file : to_index_files) {
        /* LOG(DEBUG) << "Buiding index for " << file.location; */
        status = BuildIndex(file);
        if (!status.ok()) {
            bg_error_ = status;
            return;
        }
    }
    /* LOG(DEBUG) << "All Buiding index Done"; */

    bg_build_index_started_ = false;
    bg_build_index_finish_signal_.notify_all();
}

template<typename EngineT>
Status DBImpl<EngineT>::TryBuildIndex() {
    if (bg_build_index_started_) return Status::OK();
    if (shutting_down_.load(std::memory_order_acquire)) return Status::OK();
    bg_build_index_started_ = true;
    std::thread build_index_task(&DBImpl<EngineT>::BackgroundBuildIndex, this);
    build_index_task.detach();
    return Status::OK();
}

template<typename EngineT>
void DBImpl<EngineT>::BackgroundCompaction() {
    std::vector<std::string> table_ids;
    pMemMgr_->Serialize(table_ids);

    Status status;
    for (auto table_id : table_ids) {
        status = BackgroundMergeFiles(table_id);
        if (!status.ok()) {
            bg_error_ = status;
            return;
        }
    }
}

template<typename EngineT>
Status DBImpl<EngineT>::DropAll() {
    return pMeta_->DropAll();
}

template<typename EngineT>
Status DBImpl<EngineT>::Size(long& result) {
    return  pMeta_->Size(result);
}

template<typename EngineT>
DBImpl<EngineT>::~DBImpl() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        shutting_down_.store(true, std::memory_order_release);
        while (bg_compaction_scheduled_) {
            bg_work_finish_signal_.wait(lock);
        }
    }
    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);
        while (bg_build_index_started_) {
            bg_build_index_finish_signal_.wait(lock);
        }
    }
    bg_timer_thread_.join();
    std::vector<std::string> ids;
    pMemMgr_->Serialize(ids);
    env_->Stop();
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
