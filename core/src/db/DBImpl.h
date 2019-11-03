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

#pragma once

#include "DB.h"
#include "Types.h"
#include "src/db/insert/MemManager.h"
#include "utils/ThreadPool.h"

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace milvus {
namespace engine {

class Env;

namespace meta {
class Meta;
}

class DBImpl : public DB {
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
    CreateTable(meta::TableSchema& table_schema) override;

    Status
    DeleteTable(const std::string& table_id, const meta::DatesT& dates) override;

    Status
    DescribeTable(meta::TableSchema& table_schema) override;

    Status
    HasTable(const std::string& table_id, bool& has_or_not) override;

    Status
    AllTables(std::vector<meta::TableSchema>& table_schema_array) override;

    Status
    PreloadTable(const std::string& table_id) override;

    Status
    UpdateTableFlag(const std::string& table_id, int64_t flag);

    Status
    GetTableRowCount(const std::string& table_id, uint64_t& row_count) override;

    Status
    InsertVectors(const std::string& table_id, uint64_t n, const float* vectors, IDNumbers& vector_ids) override;

    Status
    CreateIndex(const std::string& table_id, const TableIndex& index) override;

    Status
    DescribeIndex(const std::string& table_id, TableIndex& index) override;

    Status
    DropIndex(const std::string& table_id) override;

    Status
    Query(const std::string& table_id, uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
          ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    Query(const std::string& table_id, uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
          const meta::DatesT& dates, ResultIds& result_ids, ResultDistances& result_distances) override;

    Status
    Query(const std::string& table_id, const std::vector<std::string>& file_ids, uint64_t k, uint64_t nq,
          uint64_t nprobe, const float* vectors, const meta::DatesT& dates, ResultIds& result_ids,
          ResultDistances& result_distances) override;

    Status
    Size(uint64_t& result) override;

 private:
    Status
    QueryAsync(const std::string& table_id, const meta::TableFilesSchema& files, uint64_t k, uint64_t nq,
               uint64_t nprobe, const float* vectors, ResultIds& result_ids, ResultDistances& result_distances);

    void
    BackgroundTimerTask();
    void
    WaitMergeFileFinish();
    void
    WaitBuildIndexFinish();

    void
    StartMetricTask();

    void
    StartCompactionTask();
    Status
    MergeFiles(const std::string& table_id, const meta::DateT& date, const meta::TableFilesSchema& files);
    Status
    BackgroundMergeFiles(const std::string& table_id);
    void
    BackgroundCompaction(std::set<std::string> table_ids);

    void
    StartBuildIndexTask(bool force = false);
    void
    BackgroundBuildIndex();

    Status
    MemSerialize();

 private:
    const DBOptions options_;

    std::atomic<bool> shutting_down_;

    std::thread bg_timer_thread_;

    meta::MetaPtr meta_ptr_;
    MemManagerPtr mem_mgr_;
    std::mutex mem_serialize_mutex_;

    ThreadPool compact_thread_pool_;
    std::mutex compact_result_mutex_;
    std::list<std::future<void>> compact_thread_results_;
    std::set<std::string> compact_table_ids_;

    ThreadPool index_thread_pool_;
    std::mutex index_result_mutex_;
    std::list<std::future<void>> index_thread_results_;

    std::mutex build_index_mutex_;
};  // DBImpl

}  // namespace engine
}  // namespace milvus
