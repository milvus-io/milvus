/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>
#include "DB.h"
#include "MemManager.h"
#include "Types.h"
#include "FaissExecutionEngine.h"
#include "Traits.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

namespace meta {
    class Meta;
}

template <typename EngineT>
class DBImpl : public DB {
public:
    typedef typename meta::Meta::Ptr MetaPtr;
    typedef typename MemManager<EngineT>::Ptr MemManagerPtr;

    DBImpl(const Options& options);

    virtual Status add_group(meta::GroupSchema& group_info) override;
    virtual Status get_group(meta::GroupSchema& group_info) override;
    virtual Status delete_vectors(const std::string& group_id, const meta::DatesT& dates) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) override;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   meta::GroupFilesSchema& group_files_info_) override;

    virtual Status add_vectors(const std::string& group_id_,
            size_t n, const float* vectors, IDNumbers& vector_ids_) override;

    virtual Status search(const std::string& group_id, size_t k, size_t nq,
            const float* vectors, QueryResults& results) override;

    virtual Status search(const std::string& group_id, size_t k, size_t nq,
            const float* vectors, const meta::DatesT& dates, QueryResults& results) override;

    virtual Status drop_all() override;

    virtual Status count(const std::string& group_id, long& result) override;

    virtual ~DBImpl();

private:

    void background_build_index();
    Status build_index(const meta::GroupFileSchema&);
    Status try_build_index();
    Status merge_files(const std::string& group_id,
            const meta::DateT& date,
            const meta::GroupFilesSchema& files);
    Status background_merge_files(const std::string& group_id);

    void try_schedule_compaction();
    void start_timer_task(int interval_);
    void background_timer_task(int interval_);

    static void BGWork(void* db);
    void background_call();
    void background_compaction();

    Env* const _env;
    const Options _options;

    std::mutex _mutex;
    std::condition_variable _bg_work_finish_signal;
    bool _bg_compaction_scheduled;
    Status _bg_error;
    std::atomic<bool> _shutting_down;

    std::mutex build_index_mutex_;
    bool bg_build_index_started_;
    std::condition_variable bg_build_index_finish_signal_;

    std::thread bg_timer_thread_;

    MetaPtr _pMeta;
    MemManagerPtr _pMemMgr;

}; // DBImpl


} // namespace engine
} // namespace vecwise
} // namespace zilliz

#include "DBImpl.cpp"
