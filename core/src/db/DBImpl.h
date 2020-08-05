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

#include "config/ConfigMgr.h"
#include "utils/ThreadPool.h"
#include "wal/WalManager.h"

namespace milvus {
namespace engine {

class DBImpl : public DB, public ConfigObserver {
 public:
    explicit DBImpl(const DBOptions& options);

    ~DBImpl();

    Status
    Start();

    Status
    Stop();

    Status
    CreateCollection(const snapshot::CreateCollectionContext& context) override;

    Status
    DropCollection(const std::string& name) override;

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
    DropIndex(const std::string& collection_name, const std::string& field_name = "") override;

    Status
    DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) override;

    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk) override;

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                  DataChunkPtr& data_chunk) override;

    Status
    DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids) override;

    Status
    Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) override;

    Status
    ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) override;

    Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force = false) override;

    Status
    Flush(const std::string& collection_name) override;

    Status
    Flush() override;

    Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold = 0.0) override;

    void
    ConfigUpdate(const std::string& name) override;

 private:
    void
    InternalFlush(const std::string& collection_name = "");

    void
    TimingFlushThread();

    void
    StartMetricTask();

    void
    TimingMetricThread();

    void
    StartBuildIndexTask(const std::vector<std::string>& collection_names);

    void
    BackgroundBuildIndexTask(std::vector<std::string> collection_names);

    void
    TimingIndexThread();

    void
    WaitBuildIndexFinish();

    void
    TimingWalThread();

    Status
    ExecWalRecord(const wal::MXLogRecord& record);

    void
    StartMergeTask(const std::set<std::string>& collection_names, bool force_merge_all = false);

    void
    BackgroundMerge(std::set<std::string> collection_names, bool force_merge_all);

    void
    WaitMergeFileFinish();

    void
    SuspendIfFirst();

    void
    ResumeIfLast();

 private:
    DBOptions options_;
    std::atomic<bool> initialized_;

    MemManagerPtr mem_mgr_;
    MergeManagerPtr merge_mgr_ptr_;

    //    std::shared_ptr<wal::WalManager> wal_mgr_;
    std::thread bg_wal_thread_;

    std::thread bg_flush_thread_;
    std::thread bg_metric_thread_;
    std::thread bg_index_thread_;

    SimpleWaitNotify swn_wal_;
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

    std::mutex build_index_mutex_;

    std::mutex flush_merge_compact_mutex_;

    int64_t live_search_num_ = 0;
    std::mutex suspend_build_mutex_;
};  // SSDBImpl

using DBImplPtr = std::shared_ptr<DBImpl>;

}  // namespace engine
}  // namespace milvus
