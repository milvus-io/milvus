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
<<<<<<< HEAD
    CountEntities(const std::string& collection_name, int64_t& row_count) override;
=======
    PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                      bool force = false) override;

    Status
    ReLoadSegmentsDeletedDocs(const std::string& collection_id, const std::vector<int64_t>& segment_ids) override;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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
<<<<<<< HEAD
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force) override;

    Status
    Flush(const std::string& collection_name) override;
=======
    Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
            double threshold = 0.0) override;

    Status
    GetVectorsByID(const engine::meta::CollectionSchema& collection, const IDNumbers& id_array,
                   std::vector<engine::VectorsData>& vectors) override;

    Status
    GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) override;

    //    Status
    //    Merge(const std::set<std::string>& collection_ids) override;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    Status
    Flush() override;

    // Note: the threshold is percent of deleted entities that trigger compact action,
    // default is 0.0, means compact will create a new segment even only one entity is deleted
    Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) override;

    void
    ConfigUpdate(const std::string& name) override;

 private:
<<<<<<< HEAD
    void
    InternalFlush(const std::string& collection_name = "", bool merge = true);
=======
    Status
    QueryAsync(const std::shared_ptr<server::Context>& context, meta::FilesHolder& files_holder, uint64_t k,
               const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
               ResultDistances& result_distances);

    Status
    HybridQueryAsync(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                     meta::FilesHolder& files_holder, context::HybridSearchContextPtr hybrid_search_context,
                     query::GeneralQueryPtr general_query,
                     std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type, uint64_t& nq,
                     ResultIds& result_ids, ResultDistances& result_distances);

    Status
    GetVectorsByIdHelper(const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors,
                         meta::FilesHolder& files_holder);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    void
    TimingFlushThread();

    void
    StartMetricTask();

    void
    TimingMetricThread();

    void
    StartBuildIndexTask(const std::vector<std::string>& collection_names, bool reset_retry_times);

    void
    BackgroundBuildIndexTask(std::vector<std::string> collection_names);

    void
    TimingIndexThread();

    void
    WaitBuildIndexFinish();

    void
    StartMergeTask(const std::set<int64_t>& collection_ids, bool force_merge_all = false);

    void
<<<<<<< HEAD
    BackgroundMerge(std::set<int64_t> collection_ids, bool force_merge_all);

    void
    WaitMergeFileFinish();
=======
    StartMergeTask(const std::set<std::string>& merge_collection_ids, bool force_merge_all = false);

    void
    BackgroundMerge(std::set<std::string> collection_ids, bool force_merge_all);

    //    Status
    //    MergeHybridFiles(const std::string& table_id, meta::FilesHolder& files_holder);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    void
    SuspendIfFirst();

    void
<<<<<<< HEAD
    ResumeIfLast();
=======
    BackgroundBuildIndex();

    Status
    CompactFile(const meta::SegmentSchema& file, double threshold, meta::SegmentsSchema& files_to_update);

    Status
    GetFilesToBuildIndex(const std::string& collection_id, const std::vector<int>& file_types,
                         meta::FilesHolder& files_holder);

    Status
    GetPartitionByTag(const std::string& collection_id, const std::string& partition_tag, std::string& partition_name);

    Status
    GetPartitionsByTags(const std::string& collection_id, const std::vector<std::string>& partition_tags,
                        std::set<std::string>& partition_name_array);

    Status
    UpdateCollectionIndexRecursively(const std::string& collection_id, const CollectionIndex& index);

    Status
    WaitCollectionIndexRecursively(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                                   const CollectionIndex& index);

    Status
    DropCollectionIndexRecursively(const std::string& collection_id);

    Status
    GetCollectionRowCountRecursively(const std::string& collection_id, uint64_t& row_count);

    Status
    ExecWalRecord(const wal::MXLogRecord& record);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    void
    MarkIndexFailedSegments(snapshot::ID_TYPE collection_id, const snapshot::IDS_TYPE& failed_ids);

    void
    IgnoreIndexFailedSegments(snapshot::ID_TYPE collection_id, snapshot::IDS_TYPE& segment_ids);

 private:
    DBOptions options_;
    std::atomic<bool> initialized_;

    MemManagerPtr mem_mgr_;
    MergeManagerPtr merge_mgr_ptr_;

    std::thread bg_flush_thread_;
    std::thread bg_metric_thread_;
    std::thread bg_index_thread_;

<<<<<<< HEAD
=======
    struct SimpleWaitNotify {
        bool notified_ = false;
        std::mutex mutex_;
        std::condition_variable cv_;

        void
        Wait() {
            std::unique_lock<std::mutex> lck(mutex_);
            if (!notified_) {
                cv_.wait(lck);
            }
            notified_ = false;
        }

        std::cv_status
        Wait_Until(const std::chrono::system_clock::time_point& tm_pint) {
            std::unique_lock<std::mutex> lck(mutex_);
            std::cv_status ret = std::cv_status::timeout;
            if (!notified_) {
                ret = cv_.wait_until(lck, tm_pint);
            }
            notified_ = false;
            return ret;
        }

        std::cv_status
        Wait_For(const std::chrono::system_clock::duration& tm_dur) {
            std::unique_lock<std::mutex> lck(mutex_);
            std::cv_status ret = std::cv_status::timeout;
            if (!notified_) {
                ret = cv_.wait_for(lck, tm_dur);
            }
            notified_ = false;
            return ret;
        }

        void
        Notify() {
            std::unique_lock<std::mutex> lck(mutex_);
            notified_ = true;
            lck.unlock();
            cv_.notify_one();
        }
    };

    SimpleWaitNotify swn_wal_;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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

    using SegmentIndexRetryMap = std::unordered_map<snapshot::ID_TYPE, int64_t>;
    using CollectionIndexRetryMap = std::unordered_map<snapshot::ID_TYPE, SegmentIndexRetryMap>;
    CollectionIndexRetryMap index_retry_map_;
    std::mutex index_retry_mutex_;

    std::mutex build_index_mutex_;

    std::mutex flush_merge_compact_mutex_;

    int64_t live_search_num_ = 0;
    std::mutex suspend_build_mutex_;
};  // SSDBImpl

using DBImplPtr = std::shared_ptr<DBImpl>;

}  // namespace engine
}  // namespace milvus
