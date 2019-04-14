#ifndef VECENGINE_DB_IMPL_H_
#define VECENGINE_DB_IMPL_H_

#include <mutex>
#include <condition_variable>
#include "db.h"

namespace vecengine {

class Env;

class DBImpl : public DB {
public:
    DBImpl(const Options& options_, const std::string& name_);

    virtual Status add_group(GroupOptions options_,
            const std::string& group_id_,
            std::string& gid_) override;

    void try_schedule_compaction();

    virtual ~DBImpl();
private:

    static void BGWork(void* db);
    void background_call();
    void background_compaction();

    Status meta_add_group(const std::string& group_id_);
    Status meta_add_group_file(const std::string& group_id_);

    const _dbname;
    Env* const _env;
    const Options _options;

    std::mutex _mutex;
    std::condition_variable _bg_work_finish_signal;
    bool _bg_compaction_scheduled;
    Status _bg_error;

}; // DBImpl

} // namespace vecengine

#endif // VECENGINE_DB_IMPL_H_
