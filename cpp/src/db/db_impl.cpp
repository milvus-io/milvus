#include <assert.h>
#include <chrono>
#include "db_impl.h"

namespace vecengine {

DBImpl::DBImpl(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_),
      _bg_work_finish_signal(_mutex),
      _bg_compaction_scheduled(false),
      _shutting_down(false),
      _pMeta(new DBMetaImpl(*(_options.pMetaOptions))),
      _pMemMgr(new MemManager(_pMeta)) {
    start_timer_task(Options.memory_sync_interval);
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

Status DBImpl::add_vectors(const std::string& group_id_,
        size_t n, const float* vectors, IDNumbers& vector_ids_) {
    Status status = _pMemMgr->add_vectors(group_id_, n, vectors, vector_ids_);
    if (!status.ok()) {
        return status;
    }
}

void DBImpl::start_timer_task(int interval_) {
    std::thread bg_task(&DBImpl::background_timer_task, this, interval_);
    bg_task.detach();
}

void DBImpl::background_timer_task(int interval_) {
    Status status;
    while (true) {
        if (!_bg_error.ok()) break;
        if (_shutting_down.load(std::memory_order_acquire)) break;

        std::this_thread::sleep_for(std::chrono::seconds(interval_));

        try_schedule_compaction();
    }
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

    _bg_compaction_scheduled = false;
    _bg_work_finish_signal.notify_all();
}

void DBImpl::background_compaction() {
    _pMemMgr->serialize();
}

DBImpl::~DBImpl() {
    std::lock_guard<std::mutex> _mutex;
    _shutting_down.store(true, std::memory_order_release);
    while (_bg_compaction_scheduled) {
        _bg_work_finish_signal.wait();
    }
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
