#include <assert.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_io.h>
#include <faiss/AutoTune.h>
#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Env.h"

namespace zilliz {
namespace vecwise {
namespace engine {

DBImpl::DBImpl(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_),
      _bg_compaction_scheduled(false),
      _shutting_down(false),
      bg_build_index_started_(false),
      _pMeta(new meta::DBMetaImpl(_options.meta)),
      _pMemMgr(new MemManager(_pMeta)) {
    start_timer_task(options_.memory_sync_interval);
}

Status DBImpl::add_group(const GroupOptions& options,
        const std::string& group_id,
        meta::GroupSchema& group_info) {
    assert((!options.has_id) ||
            (options.has_id && ("" != group_id)));

    return _pMeta->add_group(options, group_id, group_info);
}

Status DBImpl::get_group(const std::string& group_id_, meta::GroupSchema& group_info_) {
    return _pMeta->get_group(group_id_, group_info_);
}

Status DBImpl::has_group(const std::string& group_id_, bool& has_or_not_) {
    return _pMeta->has_group(group_id_, has_or_not_);
}

Status DBImpl::get_group_files(const std::string& group_id,
                               const int date_delta,
                               meta::GroupFilesSchema& group_files_info) {
    return _pMeta->get_group_files(group_id, date_delta, group_files_info);

}

Status DBImpl::add_vectors(const std::string& group_id_,
        size_t n, const float* vectors, IDNumbers& vector_ids_) {
    Status status = _pMemMgr->add_vectors(group_id_, n, vectors, vector_ids_);
    if (!status.ok()) {
        return status;
    }
}

Status DBImpl::search(const std::string& group_id, size_t k, size_t nq,
        const float* vectors, QueryResults& results) {
    // PXU TODO
    return Status::OK();
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


Status DBImpl::merge_files(const std::string& group_id, const meta::DateT& date,
        const meta::GroupFilesSchema& files) {
    meta::GroupFileSchema group_file;
    Status status = _pMeta->add_group_file(group_id, date, group_file);
    if (!status.ok()) {
        return status;
    }

    std::shared_ptr<faiss::Index> index(faiss::index_factory(group_file.dimension, "IDMap,Flat"));

    meta::GroupFilesSchema updated;

    for (auto& file : files) {
        auto file_index = dynamic_cast<faiss::IndexIDMap*>(faiss::read_index(file.location.c_str()));
        index->add_with_ids(file_index->ntotal, dynamic_cast<faiss::IndexFlat*>(file_index->index)->xb.data(),
                file_index->id_map.data());
        auto file_schema = file;
        file_schema.file_type = meta::GroupFileSchema::TO_DELETE;
        updated.push_back(file_schema);
    }

    faiss::write_index(index.get(), group_file.location.c_str());
    group_file.file_type = meta::GroupFileSchema::RAW;
    updated.push_back(group_file);
    status = _pMeta->update_files(updated);

    return status;
}

Status DBImpl::background_merge_files(const std::string& group_id) {
    meta::DatePartionedGroupFilesSchema raw_files;
    auto status = _pMeta->files_to_merge(group_id, raw_files);
    if (!status.ok()) {
        return status;
    }

    if (raw_files.size() == 0) {
        return Status::OK();
    }

    bool has_merge = false;

    for (auto& kv : raw_files) {
        auto files = kv.second;
        if (files.size() <= _options.raw_file_merge_trigger_number) {
            continue;
        }
        has_merge = true;
        merge_files(group_id, kv.first, kv.second);
    }

    if (has_merge) {
        try_build_index();
    }

    return Status::OK();
}

Status DBImpl::build_index(const meta::GroupFileSchema& file) {
    //PXU TODO
    std::cout << ">>Building Index for: " << file.location << std::endl;
    return Status::OK();
}

Status DBImpl::background_build_index() {
    assert(bg_build_index_started_);
    meta::GroupFilesSchema to_index_files;
    _pMeta->files_to_index(to_index_files);
    Status status;
    for (auto& file : to_index_files) {
        status = build_index(file);
        if (!status.ok()) {
            _bg_error = status;
            return status;
        }
    }

    bg_build_index_started_ = false;
    return Status::OK();
}

Status DBImpl::try_build_index() {
    if (bg_build_index_started_) return Status::OK();
    bg_build_index_started_ = true;
    std::thread build_index_task(&DBImpl::background_build_index, this);
    build_index_task.detach();
    return Status::OK();
}

void DBImpl::background_compaction() {
    std::vector<std::string> group_ids;
    _pMemMgr->serialize(group_ids);

    Status status;
    for (auto group_id : group_ids) {
        /* std::cout << __func__ << " group_id=" << group_id << std::endl; */
        status = background_merge_files(group_id);
        if (!status.ok()) {
            _bg_error = status;
            return;
        }
    }
}

DBImpl::~DBImpl() {
    std::unique_lock<std::mutex> lock(_mutex);
    _shutting_down.store(true, std::memory_order_release);
    while (_bg_compaction_scheduled) {
        _bg_work_finish_signal.wait(lock);
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

} // namespace engine
} // namespace vecwise
} // namespace zilliz
