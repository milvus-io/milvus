#include <assert.h>
#include "db_impl.h"

namespace vecengine {

DBImpl::DBImpl(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_),
      _bg_work_finish_signal(_mutex),
      _bg_compaction_scheduled(false) {
}

Status DBImpl::add_group(const GroupOptions& options_,
        const std::string& group_id_,
        std::string& gid_) {
    assert((!options_.has_id) ||
            (options_.has_id && ("" != group_id_)));

}

void DBImpl::try_schedule_compaction() {
    if (_bg_compaction_scheduled) return;
    if (!_bg_error.ok()) return;

    _bg_compaction_scheduled = true;
    _env->schedule(&DBImpl::BGWork, this);
}

void DBImpl::BGWork(void* db_) {
    reinterpret_cast<DBImpl*>(db_)->background_call();
}

void DBImpl::background_call() {
    std::lock_guard<std::mutex> lock(_mutex);
    assert(_bg_compaction_scheduled);

    if (!_bg_error.ok()) return;

    background_compaction();
}

void DBImpl::background_compaction() {

}

void DBImpl::compact_memory() {

}

/*
 *  DB
 */

DB::~DB() {}

DB* DB::Open(const Options& options_, const std::string& name_) {
    DBImpl* impl = new DBImpl(options_, name_);
    return impl;
}

} // namespace vecengine
