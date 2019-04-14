#include <assert.h>
#include "db_impl.h"

namespace vecengine {

DBImpl::DBImpl(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_),
      _bg_work_finish_signal(_mutex),
      _bg_compaction_scheduled(false),
      _pMeta(new DBMetaImpl(*(_options.pMetaOptions))) {
}

Status DBImpl::add_group(const GroupOptions& options_,
        const std::string& group_id_,
        GroupSchema& group_info_) {
    assert((!options_.has_id) ||
            (options_.has_id && ("" != group_id_)));

    return _pMeta->add_group(options_, group_id, group_info_);
}

Status DBImpl::get_group(const std::string& group_id_, GroupSchema& group_info_) {
    return _pMeta->get_group(group_id_, group_info_);
}

Status DBImpl::has_group(const std::string& group_id_, bool& has_or_not_) {
    return _pMeta->has_group(group_id_, has_or_not_);
}

Status DBImpl::get_group_files(const std::string& group_id_,
                               const int date_delta_,
                               GroupFilesSchema& group_files_info_) {
    return _pMeta->get_group_files(group_id_, date_delta_, group_file_info_);

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
