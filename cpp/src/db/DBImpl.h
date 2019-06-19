/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "DB.h"
#include "MemManager.h"
#include "Types.h"

#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>
#include <thread>

namespace zilliz {
namespace milvus {
namespace engine {

class Env;

namespace meta {
    class Meta;
}

class DBImpl : public DB {
public:
    using MetaPtr = meta::Meta::Ptr;
    using MemManagerPtr = typename MemManager::Ptr;

    DBImpl(const Options& options);

    virtual Status CreateTable(meta::TableSchema& table_schema) override;
    virtual Status DeleteTable(const std::string& table_id, const meta::DatesT& dates) override;
    virtual Status DescribeTable(meta::TableSchema& table_schema) override;
    virtual Status HasTable(const std::string& table_id, bool& has_or_not) override;
    virtual Status AllTables(std::vector<meta::TableSchema>& table_schema_array) override;
    virtual Status GetTableRowCount(const std::string& table_id, uint64_t& row_count) override;

    virtual Status InsertVectors(const std::string& table_id,
                                 uint64_t n, const float* vectors, IDNumbers& vector_ids) override;

    virtual Status Query(const std::string& table_id, uint64_t k, uint64_t nq,
            const float* vectors, QueryResults& results) override;

    virtual Status Query(const std::string& table_id, uint64_t k, uint64_t nq,
            const float* vectors, const meta::DatesT& dates, QueryResults& results) override;

    virtual Status Query(const std::string& table_id, const std::vector<std::string>& file_ids,
                         uint64_t k, uint64_t nq, const float* vectors,
                         const meta::DatesT& dates, QueryResults& results) override;

    virtual Status DropAll() override;

    virtual Status Size(uint64_t& result) override;

    virtual ~DBImpl();

private:
    Status QuerySync(const std::string& table_id, uint64_t k, uint64_t nq,
            const float* vectors, const meta::DatesT& dates, QueryResults& results);

    Status QueryAsync(const std::string& table_id, const meta::TableFilesSchema& files,
            uint64_t k, uint64_t nq, const float* vectors,
            const meta::DatesT& dates, QueryResults& results);


    void BackgroundBuildIndex();
    Status BuildIndex(const meta::TableFileSchema&);
    Status TryBuildIndex();
    Status MergeFiles(const std::string& table_id,
            const meta::DateT& date,
            const meta::TableFilesSchema& files);
    Status BackgroundMergeFiles(const std::string& table_id);

    void TrySchedule();
    void StartTimerTasks(int interval);
    void BackgroundTimerTask(int interval);

    static void BGWork(void* db);
    void BackgroundCall();
    void BackgroundCompaction();

    Env* const env_;
    const Options options_;

    std::mutex mutex_;
    std::condition_variable bg_work_finish_signal_;
    bool bg_compaction_scheduled_;
    Status bg_error_;
    std::atomic<bool> shutting_down_;

    std::mutex build_index_mutex_;
    bool bg_build_index_started_;
    std::condition_variable bg_build_index_finish_signal_;

    std::thread bg_timer_thread_;

    MetaPtr pMeta_;
    MemManagerPtr pMemMgr_;

}; // DBImpl


} // namespace engine
} // namespace milvus
} // namespace zilliz
