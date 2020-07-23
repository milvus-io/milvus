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
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/ConfigMgr.h"
#include "db/DB.h"
#include "db/IndexFailedChecker.h"
#include "db/SimpleWaitNotify.h"
#include "db/Types.h"
#include "db/insert/MemManager.h"
#include "db/merge/MergeManager.h"
#include "db/meta/FilesHolder.h"
#include "db/snapshot/Context.h"
#include "utils/ThreadPool.h"
#include "wal/WalManager.h"

namespace milvus {
namespace engine {

namespace meta {
class Meta;
}

class DBImpl : public DB, public ConfigObserver {
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
    CreateCollection(meta::CollectionSchema& collection_schema) override;

    Status
    DropCollection(const std::string& collection_id) override;

    Status
    DescribeCollection(meta::CollectionSchema& collection_schema) override;

    Status
    HasCollection(const std::string& collection_id, bool& has_or_not) override;

    Status
    HasNativeCollection(const std::string& collection_id, bool& has_or_not_) override;

    Status
    AllCollections(std::vector<std::string>& names) override;

    Status
    GetCollectionInfo(const std::string& collection_id, std::string& collection_info) override;

    Status
    PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                      bool force = false) override;

    Status
    ReLoadSegmentsDeletedDocs(const std::string& collection_id, const std::vector<int64_t>& segment_ids) override;

    Status
    UpdateCollectionFlag(const std::string& collection_id, int64_t flag) override;

    Status
    GetCollectionRowCount(const std::string& collection_id, uint64_t& row_count) override;

    Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name,
                    const std::string& partition_tag) override;

    Status
    HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) override;

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
    DeleteEntities(const std::string& collection_id, IDNumbers entity_ids) override;

    Status
    Flush(const std::string& collection_id) override;

    Status
    Flush() override;

    Status
    Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
            double threshold = 0.0) override;

    Status
    GetVectorsByID(const engine::meta::CollectionSchema& collection, const IDNumbers& id_array,
                   std::vector<engine::VectorsData>& vectors) override;

    Status
    GetEntitiesByID(const std::string& collection_id, const IDNumbers& id_array,
                    const std::vector<std::string>& field_names, std::vector<engine::VectorsData>& vectors,
                    std::vector<engine::AttrsData>& attrs) override;

    Status
    GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) override;

    //    Status
    //    Merge(const std::set<std::string>& collection_ids) override;

    Status
    CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                const CollectionIndex& index) override;

    Status
    CreateStructuredIndex(const std::string& collection_id, const std::vector<std::string>& field_names,
                          const std::unordered_map<std::string, meta::hybrid::DataType>& attr_types,
                          const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data,
                          std::unordered_map<std::string, int64_t>& attr_size,
                          std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes) override;

    Status
    DescribeIndex(const std::string& collection_id, CollectionIndex& index) override;

    Status
    DropIndex(const std::string& collection_id) override;

    Status
    CreateHybridCollection(meta::CollectionSchema& collection_schema,
                           meta::hybrid::FieldsSchema& fields_schema) override;

    Status
    DescribeHybridCollection(meta::CollectionSchema& collection_schema,
                             meta::hybrid::FieldsSchema& fields_schema) override;

    Status
    InsertEntities(const std::string& collection_name, const std::string& partition_tag,
                   const std::vector<std::string>& field_names, engine::Entity& entity,
                   std::unordered_map<std::string, meta::hybrid::DataType>& field_types) override;

    Status
    HybridQuery(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                const std::vector<std::string>& partition_tags, query::GeneralQueryPtr general_query,
                query::QueryPtr query_ptr, std::vector<std::string>& field_names,
                std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                engine::QueryResult& result) override;

    Status
    QueryByIDs(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
               const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
               const IDNumbers& id_array, ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
          const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
          VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    QueryByFileID(const std::shared_ptr<server::Context>& context, const std::vector<std::string>& file_ids, uint64_t k,
                  const milvus::json& extra_params, VectorsData& vectors, ResultIds& result_ids,
                  ResultDistances& result_distances) override;

    Status
    Size(uint64_t& result) override;

    Status
    FlushAttrsIndex(const std::string& collection_id) override;

 public:
    void
    ConfigUpdate(const std::string& name) override;

 private:
    Status
    QueryAsync(const std::shared_ptr<server::Context>& context, meta::FilesHolder& files_holder, uint64_t k,
               const milvus::json& extra_params, VectorsData& vectors, ResultIds& result_ids,
               ResultDistances& result_distances);

    Status
    HybridQueryAsync(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                     meta::FilesHolder& files_holder, query::GeneralQueryPtr general_query, query::QueryPtr query_ptr,
                     std::vector<std::string>& field_names,
                     std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                     engine::QueryResult& result);

    Status
    GetVectorsByIdHelper(const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors,
                         meta::FilesHolder& files_holder);

    Status
    GetEntitiesByIdHelper(const std::string& collection_id, const IDNumbers& id_array,
                          const std::vector<std::string>& field_names,
                          std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                          std::vector<engine::VectorsData>& vectors, std::vector<engine::AttrsData>& attrs,
                          meta::FilesHolder& files_holder);

    void
    InternalFlush(const std::string& collection_id = "");

    void
    BackgroundWalThread();

    void
    BackgroundFlushThread();

    void
    BackgroundMetricThread();

    void
    BackgroundIndexThread();

    void
    WaitMergeFileFinish();

    void
    WaitBuildIndexFinish();

    void
    StartMetricTask();

    void
    StartMergeTask(const std::set<std::string>& merge_collection_ids, bool force_merge_all = false);

    void
    BackgroundMerge(std::set<std::string> collection_ids, bool force_merge_all);

    //    Status
    //    MergeHybridFiles(const std::string& table_id, meta::FilesHolder& files_holder);

    void
    StartBuildIndexTask();

    void
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

    void
    SuspendIfFirst();

    void
    ResumeIfLast();

    Status
    SerializeStructuredIndex(const meta::SegmentSchema& segment_schema,
                             const std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes,
                             const std::unordered_map<std::string, int64_t>& attr_sizes,
                             const std::unordered_map<std::string, meta::hybrid::DataType>& attr_types);

    ///////////////////////////////////////////////////////////////////////////////
    /* Mocked SSDBImpl interfaces, will be removed */
    Status
    CreateCollection(const snapshot::CreateCollectionContext& context) override {
        assert(false);
        return Status::OK();
    }

    // Status
    // DropCollection(const std::string& name);

    Status
    DescribeCollection(const std::string& collection_name, snapshot::CollectionPtr& collection,
                       std::map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) override {
        assert(false);
        return Status::OK();
    }

    // Status
    // HasCollection(const std::string& collection_name, bool& has_or_not);

    // Status
    // AllCollections(std::vector<std::string>& names);

    // Status
    // GetCollectionRowCount(const std::string& collection_name, uint64_t& row_count);

    Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force = false) override {
        assert(false);
        return Status::OK();
    }

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name) override {
        assert(false);
        return Status::OK();
    }

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name) override {
        assert(false);
        return Status::OK();
    }

    Status
    ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) override {
        assert(false);
        return Status::OK();
    }

    Status
    InsertEntities(const std::string& collection_name, const std::string& partition_name,
                   DataChunkPtr& data_chunk) override {
        assert(false);
        return Status::OK();
    }

    // Status
    // DeleteEntities(const std::string& collection_name, engine::IDNumbers entity_ids);

    // Status
    // Flush(const std::string& collection_name);

    // Status
    // Flush();

    // Status
    // Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold = 0.0);

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, DataChunkPtr& data_chunk) override {
        assert(false);
        return Status::OK();
    }

    Status
    GetEntityIDs(const std::string& collection_id, int64_t segment_id, IDNumbers& entity_ids) override {
        assert(false);
        return Status::OK();
    }

    Status
    CreateIndex(const server::ContextPtr& context, const std::string& collection_id, const std::string& field_name,
                const CollectionIndex& index) override {
        assert(false);
        return Status::OK();
    }

    Status
    DescribeIndex(const std::string& collection_id, const std::string& field_name, CollectionIndex& index) override {
        assert(false);
        return Status::OK();
    }

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name) override {
        assert(false);
        return Status::OK();
    }

    // Status
    // DropIndex(const std::string& collection_id);

    Status
    Query(const server::ContextPtr& context, const std::string& collection_name, const query::QueryPtr& query_ptr,
          engine::QueryResultPtr& result) override {
        assert(false);
        return Status::OK();
    }
    ///////////////////////////////////////////////////////////////////////////////

 private:
    DBOptions options_;

    std::atomic<bool> initialized_;

    meta::MetaPtr meta_ptr_;
    MemManagerPtr mem_mgr_;
    MergeManagerPtr merge_mgr_ptr_;

    std::shared_ptr<wal::WalManager> wal_mgr_;
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

    IndexFailedChecker index_failed_checker_;

    std::mutex flush_merge_compact_mutex_;

    int64_t live_search_num_ = 0;
    std::mutex suspend_build_mutex_;
};  // DBImpl

}  // namespace engine
}  // namespace milvus
