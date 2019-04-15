#ifndef VECENGINE_DB_IMPL_H_
#define VECENGINE_DB_IMPL_H_

#include <mutex>
#include <condition_variable>
#include <memory>
#include "db.h"
#include "memvectors.h"
#include "types.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DBImpl : public DB {
public:
    DBImpl(const Options& options_, const std::string& name_);

    virtual Status add_group(GroupOptions options_,
            const std::string& group_id_,
            GroupSchema& group_info_) override;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info_) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) override;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) override;

    virtual Status add_vectors(const std::string& group_id_,
            size_t n, const float* vectors, IDNumbers& vector_ids_) override;

    virtual ~DBImpl();

private:

    void try_schedule_compaction();

    static void BGWork(void* db);
    void background_call();
    void background_compaction();

    const _dbname;
    Env* const _env;
    const Options _options;

    std::mutex _mutex;
    std::condition_variable _bg_work_finish_signal;
    bool _bg_compaction_scheduled;
    Status _bg_error;
    std::atomic<bool> _shutting_down;

    std::shared_ptr<Meta> _pMeta;
    std::shared_ptr<MemManager> _pMemMgr;

}; // DBImpl

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_META_IMPL_H_

#endif // VECENGINE_DB_IMPL_H_
