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
#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "config/handler/CacheConfigHandler.h"
#include "config/handler/EngineConfigHandler.h"
#include "db/DB.h"
#include "db/IndexFailedChecker.h"
#include "db/OngoingFileChecker.h"
#include "db/Types.h"
#include "db/insert/MemManager.h"
#include "utils/ThreadPool.h"
#include "wal/WalManager.h"

namespace milvus {
namespace engine {

namespace meta {
class Meta;
}

class DBImpl : public DB, public server::CacheConfigHandler, public server::EngineConfigHandler {
 public:
    explicit DBImpl(const DBOptions& options);

    ~DBImpl();

    Status
    Start() override;

    Status
    Stop() override;

    Status
    DropAll() override;

    Status
    CreateTable(meta::CollectionSchema& table_schema) override;

    Status
    DropTable(const std::string& collection_id) override;

    Status
    DescribeTable(meta::CollectionSchema& table_schema) override;

    Status
    HasTable(const std::string& collection_id, bool& has_or_not) override;

    Status
    HasNativeTable(const std::string& collection_id, bool& has_or_not_) override;

    Status
    AllTables(std::vector<meta::CollectionSchema>& table_schema_array) override;

    Status
    GetTableInfo(const std::string& collection_id, TableInfo& table_info) override;

    Status
    PreloadTable(const std::string& collection_id) override;

    Status
    UpdateTableFlag(const std::string& collection_id, int64_t flag) override;

    Status
    GetTableRowCount(const std::string& collection_id, uint64_t& row_count) override;

    Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name,
                    const std::string& partition_tag) override;

    Status
    DropPartition(const std::string& partition_name) override;

    Status
    DropPartitionByTag(const std::string& collection_id, const std::string& partition_tag) override;

    Status
    ShowPartitions(const std::string& collection_id,
                   std::vector<meta::CollectionSchema>& partition_schema_array) override;

    Status
    InsertVectors(const std::string& collection_id, const std::string& partition_tag, VectorsData& vectors) override;

    Status
    DeleteVector(const std::string& collection_id, IDNumber vector_id) override;

    Status
    DeleteVectors(const std::string& collection_id, IDNumbers vector_ids) override;

    Status
    Flush(const std::string& collection_id) override;

    Status
    Flush() override;

    Status
    Compact(const std::string& collection_id) override;

    Status
    GetVectorByID(const std::string& collection_id, const IDNumber& vector_id, VectorsData& vector) override;

    Status
    GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) override;

    //    Status
    //    Merge(const std::set<std::string>& table_ids) override;

    Status
    CreateIndex(const std::string& collection_id, const TableIndex& index) override;

    Status
    DescribeIndex(const std::string& collection_id, TableIndex& index) override;

    Status
    DropIndex(const std::string& collection_id) override;

    Status
    QueryByID(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
              const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
              IDNumber vector_id, ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
          const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
          const VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    QueryByFileID(const std::shared_ptr<server::Context>& context, const std::vector<std::string>& file_ids, uint64_t k,
                  const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
                  ResultDistances& result_distances) override;

    Status
    Size(uint64_t& result) override;

 protected:
    void
    OnCacheInsertDataChanged(bool value) override;

    void
    OnUseBlasThresholdChanged(int64_t threshold) override;

 private:
    Status
    QueryAsync(const std::shared_ptr<server::Context>& context, const meta::SegmentsSchema& files, uint64_t k,
               const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
               ResultDistances& result_distances);

    Status
    GetVectorByIdHelper(const std::string& collection_id, IDNumber vector_id, VectorsData& vector,
                        const meta::SegmentsSchema& files);

    void
    BackgroundTimerTask();

    void
    WaitMergeFileFinish();

    void
    WaitBuildIndexFinish();

    void
    StartMetricTask();

    void
    StartMergeTask();

    Status
    MergeFiles(const std::string& collection_id, const meta::SegmentsSchema& files);

    Status
    BackgroundMergeFiles(const std::string& collection_id);

    void
    BackgroundMerge(std::set<std::string> table_ids);

    void
    StartBuildIndexTask(bool force = false);

    void
    BackgroundBuildIndex();

    Status
    CompactFile(const std::string& collection_id, const meta::SegmentSchema& file,
                meta::SegmentsSchema& files_to_update);

    /*
    Status
    SyncMemData(std::set<std::string>& sync_table_ids);
    */

    Status
    GetFilesToBuildIndex(const std::string& collection_id, const std::vector<int>& file_types,
                         meta::SegmentsSchema& files);

    Status
    GetFilesToSearch(const std::string& collection_id, meta::SegmentsSchema& files);

    Status
    GetPartitionByTag(const std::string& collection_id, const std::string& partition_tag, std::string& partition_name);

    Status
    GetPartitionsByTags(const std::string& collection_id, const std::vector<std::string>& partition_tags,
                        std::set<std::string>& partition_name_array);

    Status
    DropTableRecursively(const std::string& collection_id);

    Status
    UpdateTableIndexRecursively(const std::string& collection_id, const TableIndex& index);

    Status
    WaitTableIndexRecursively(const std::string& collection_id, const TableIndex& index);

    Status
    DropTableIndexRecursively(const std::string& collection_id);

    Status
    GetTableRowCountRecursively(const std::string& collection_id, uint64_t& row_count);

    Status
    ExecWalRecord(const wal::MXLogRecord& record);

    void
    BackgroundWalTask();

 private:
    DBOptions options_;

    std::atomic<bool> initialized_;

    std::thread bg_timer_thread_;

    meta::MetaPtr meta_ptr_;
    MemManagerPtr mem_mgr_;

    std::shared_ptr<wal::WalManager> wal_mgr_;
    std::thread bg_wal_thread_;

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

        void
        Wait_Until(const std::chrono::system_clock::time_point& tm_pint) {
            std::unique_lock<std::mutex> lck(mutex_);
            if (!notified_) {
                cv_.wait_until(lck, tm_pint);
            }
            notified_ = false;
        }

        void
        Wait_For(const std::chrono::system_clock::duration& tm_dur) {
            std::unique_lock<std::mutex> lck(mutex_);
            if (!notified_) {
                cv_.wait_for(lck, tm_dur);
            }
            notified_ = false;
        }

        void
        Notify() {
            std::unique_lock<std::mutex> lck(mutex_);
            notified_ = true;
            lck.unlock();
            cv_.notify_one();
        }
    };

    SimpleWaitNotify bg_task_swn_;
    SimpleWaitNotify flush_task_swn_;

    ThreadPool merge_thread_pool_;
    std::mutex merge_result_mutex_;
    std::list<std::future<void>> merge_thread_results_;
    std::set<std::string> merge_table_ids_;

    ThreadPool index_thread_pool_;
    std::mutex index_result_mutex_;
    std::list<std::future<void>> index_thread_results_;

    std::mutex build_index_mutex_;

    IndexFailedChecker index_failed_checker_;

    std::mutex flush_merge_compact_mutex_;
};  // DBImpl

}  // namespace engine
}  // namespace milvus
