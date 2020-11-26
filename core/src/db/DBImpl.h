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

#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/DB.h"
#include "db/SegmentTaskTracker.h"
#include "metrics/Prometheus.h"
#include "utils/ThreadPool.h"
#include "value/config/ConfigMgr.h"

namespace milvus {
namespace engine {

class DBImpl : public DB, public ConfigObserver {
 public:
    explicit DBImpl(const DBOptions& options);

    ~DBImpl();

    Status
    Start() override;

    Status
    Stop() override;

    Status
    CreateCollection(const snapshot::CreateCollectionContext& context) override;

    Status
    DropCollection(const std::string& collection_name) override;

    Status
    HasCollection(const std::string& collection_name, bool& has_or_not) override;

    Status
    ListCollections(std::vector<std::string>& names) override;

    Status
    GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                      snapshot::FieldElementMappings& fields_schema) override;

    Status
    GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) override;

    Status
    CountEntities(const std::string& collection_name, int64_t& row_count) override;

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name) override;

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name) override;

    Status
    HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) override;

    Status
    ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) override;

    Status
    CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                const std::string& field_name, const CollectionIndex& index) override;

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name) override;

    Status
    DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) override;

    // Note: the data_chunk will be consumed with this method, and only return id field to client
    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
           idx_t op_id) override;

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                  DataChunkPtr& data_chunk) override;

    Status
    DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) override;

    Status
    Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) override;

    Status
    ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) override;

    // Note: if the input field_names is empty, will load all fields of this collection
    Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force) override;

    Status
    Flush(const std::string& collection_name) override;

    Status
    Flush() override;

    // Note: the threshold is percent of deleted entities that trigger compact action,
    // default is 0.0, means compact will create a new segment even only one entity is deleted
    Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) override;

    void
    ConfigUpdate(const std::string& name) override;

 private:
    bool
    ServiceAvailable();

    void
    SetAvailable(bool available);

    void
    InternalFlush(const std::string& collection_name = "", bool merge = true);

    void
    TimingFlushThread();

    void
    StartBuildIndexTask(const std::vector<std::string>& collection_names, bool force_build);

    void
    BackgroundBuildIndexTask(std::vector<std::string> collection_names, bool force_build);

    void
    TimingIndexThread();

    void
    WaitBuildIndexFinish();

    void
    StartMergeTask(const std::set<int64_t>& collection_ids, bool force_merge_all);

    void
    BackgroundMerge(std::set<int64_t> collection_ids, bool force_merge_all);

    void
    BackgroundMetricThread();

    void
    WaitMergeFileFinish();

    void
    SuspendIfFirst();

    void
    ResumeIfLast();

    void
    IncreaseLiveBuildTaskNum();

    void
    DecreaseLiveBuildTaskNum();

 private:
    DBOptions options_;
    std::atomic<bool> available_;

    MemManagerPtr mem_mgr_;
    MergeManagerPtr merge_mgr_ptr_;

    std::thread bg_flush_thread_;
    std::thread bg_metric_thread_;
    std::thread bg_index_thread_;

    SimpleWaitNotify swn_flush_;
    SimpleWaitNotify swn_metric_;
    SimpleWaitNotify swn_index_;

    SimpleWaitNotify flush_req_swn_;
    SimpleWaitNotify index_req_swn_;

    ThreadPool merge_thread_pool_;
    std::mutex merge_result_mutex_;
    std::list<std::future<void>> merge_thread_results_;

    ThreadPool index_thread_pool_;
    std::mutex index_result_mutex_;
    std::list<std::future<void>> index_thread_results_;

    SegmentTaskTracker index_task_tracker_;

    std::mutex build_index_mutex_;

    std::mutex flush_merge_compact_mutex_;

    int64_t live_search_num_ = 0;
    std::mutex suspend_build_mutex_;

    int64_t live_build_num_ = 0;
    std::mutex live_build_count_mutex_;

    // Metrics
    prometheus::Family<prometheus::Gauge>& db_info_family_ =
        prometheus::BuildGauge().Name("milvus_db_info").Help("milvus db info").Register(prometheus.registry());
    prometheus::Gauge& data_size_gauge_ = db_info_family_.Add({{"db_info", "data_size"}});

    prometheus::Family<prometheus::Gauge>& query_info_family_ =
        prometheus::BuildGauge().Name("milvus_query_info").Help("milvus query info").Register(prometheus.registry());
    prometheus::Gauge& query_per_second_gauge_ = query_info_family_.Add({});
};  // SSDBImpl

using DBImplPtr = std::shared_ptr<DBImpl>;

}  // namespace engine
}  // namespace milvus
