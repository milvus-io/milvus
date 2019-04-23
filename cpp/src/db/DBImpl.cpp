#include <assert.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_io.h>
#include <faiss/AutoTune.h>
#include <wrapper/IndexBuilder.h>
#include <cstring>
#include <wrapper/Topk.h>
#include <easylogging++.h>
#include "DBImpl.h"
#include "DBMetaImpl.h"
#include "Env.h"

namespace zilliz {
namespace vecwise {
namespace engine {

DBImpl::DBImpl(const Options& options)
    : _env(options.env),
      _options(options),
      _bg_compaction_scheduled(false),
      _shutting_down(false),
      bg_build_index_started_(false),
      _pMeta(new meta::DBMetaImpl(_options.meta)),
      _pMemMgr(new MemManager(_pMeta, _options)) {
    start_timer_task(_options.memory_sync_interval);
}

Status DBImpl::add_group(meta::GroupSchema& group_info) {
    return _pMeta->add_group(group_info);
}

Status DBImpl::get_group(meta::GroupSchema& group_info) {
    return _pMeta->get_group(group_info);
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

// TODO(XUPENG): add search range based on time
Status DBImpl::search(const std::string &group_id, size_t k, size_t nq,
                      const float *vectors, QueryResults &results) {
    meta::DatePartionedGroupFilesSchema files;
    std::vector<meta::DateT> partition;
    auto status = _pMeta->files_to_search(group_id, partition, files);
    if (!status.ok()) { return status; }

    meta::GroupFilesSchema index_files;
    meta::GroupFilesSchema raw_files;
    for (auto &day_files : files) {
        for (auto &file : day_files.second) {
            file.file_type == meta::GroupFileSchema::INDEX ?
            index_files.push_back(file) : raw_files.push_back(file);
        }
    }

    int dim = 0;
    if (!index_files.empty()) {
        dim = index_files[0].dimension;
    } else if (!raw_files.empty()) {
        dim = raw_files[0].dimension;
    } else {
        return Status::OK();
    }

    // merge raw files and build flat index.
    faiss::Index *index(faiss::index_factory(dim, "IDMap,Flat"));
    for (auto &file : raw_files) {
        auto file_index = dynamic_cast<faiss::IndexIDMap *>(faiss::read_index(file.location.c_str()));
        index->add_with_ids(file_index->ntotal,
                            dynamic_cast<faiss::IndexFlat *>(file_index->index)->xb.data(),
                            file_index->id_map.data());
    }

    {
        // [{ids, distence}, ...]
        using SearchResult = std::pair<std::vector<long>, std::vector<float>>;
        std::vector<SearchResult> batchresult(nq); // allocate nq cells.

        auto cluster = [&](long *nns, float *dis) -> void {
            for (int i = 0; i < nq; ++i) {
                auto f_begin = batchresult[i].first.cbegin();
                auto s_begin = batchresult[i].second.cbegin();
                batchresult[i].first.insert(f_begin, nns + i * k, nns + i * k + k);
                batchresult[i].second.insert(s_begin, dis + i * k, dis + i * k + k);
            }
        };

        // Allocate Memory
        float *output_distence;
        long *output_ids;
        output_distence = (float *) malloc(k * nq * sizeof(float));
        output_ids = (long *) malloc(k * nq * sizeof(long));
        memset(output_distence, 0, k * nq * sizeof(float));
        memset(output_ids, 0, k * nq * sizeof(long));

        // search in raw file
        index->search(nq, vectors, k, output_distence, output_ids);
        cluster(output_ids, output_distence); // cluster to each query
        memset(output_distence, 0, k * nq * sizeof(float));
        memset(output_ids, 0, k * nq * sizeof(long));

        // Search in index file
        for (auto &file : index_files) {
            auto index = read_index(file.location.c_str());
            index->search(nq, vectors, k, output_distence, output_ids);
            cluster(output_ids, output_distence); // cluster to each query
            memset(output_distence, 0, k * nq * sizeof(float));
            memset(output_ids, 0, k * nq * sizeof(long));
        }

        auto cluster_topk = [&]() -> void {
            QueryResult res;
            for (auto &result_pair : batchresult) {
                auto &dis = result_pair.second;
                auto &nns = result_pair.first;
                TopK(dis.data(), dis.size(), k, output_distence, output_ids);
                for (int i = 0; i < k; ++i) {
                    res.emplace_back(nns[output_ids[i]]); // mapping
                }
                results.push_back(res); // append to result list
                res.clear();
            }
        };
        cluster_topk();

        free(output_distence);
        free(output_ids);
    }

    if (results.empty()) {
        return Status::NotFound("Group " + group_id + ", search result not found!");
    }
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
    group_file.group_id = group_id;
    group_file.date = date;
    Status status = _pMeta->add_group_file(group_file);

    if (!status.ok()) {
        LOG(INFO) << status.ToString() << std::endl;
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
        LOG(DEBUG) << "About to merge file " << file_schema.file_id <<
            " of size=" << file_schema.rows;
    }

    auto index_size = group_file.dimension * index->ntotal;
    faiss::write_index(index.get(), group_file.location.c_str());

    if (index_size >= _options.index_trigger_size) {
        group_file.file_type = meta::GroupFileSchema::TO_INDEX;
    } else {
        group_file.file_type = meta::GroupFileSchema::RAW;
    }
    group_file.rows = index_size;
    updated.push_back(group_file);
    status = _pMeta->update_files(updated);
    LOG(DEBUG) << "New merged file " << group_file.file_id <<
        " of size=" << group_file.rows;

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
        if (files.size() <= _options.merge_trigger_number) {
            continue;
        }
        has_merge = true;
        merge_files(group_id, kv.first, kv.second);
    }

    if (has_merge) {
        try_build_index();
    }

    _pMeta->cleanup_ttl_files(1);

    return Status::OK();
}

Status DBImpl::build_index(const meta::GroupFileSchema& file) {
    meta::GroupFileSchema group_file;
    group_file.group_id = file.group_id;
    group_file.date = file.date;
    Status status = _pMeta->add_group_file(group_file);
    if (!status.ok()) {
        return status;
    }

    auto opd = std::make_shared<Operand>();
    opd->d = file.dimension;
    opd->index_type = "IDMap,Flat";
    IndexBuilderPtr pBuilder = GetIndexBuilder(opd);

    auto from_index = dynamic_cast<faiss::IndexIDMap*>(faiss::read_index(file.location.c_str()));
    LOG(DEBUG) << "Preparing build_index for file_id=" << file.file_id
        << " with new index_file_id=" << group_file.file_id << std::endl;
    auto index = pBuilder->build_all(from_index->ntotal,
            dynamic_cast<faiss::IndexFlat*>(from_index->index)->xb.data(),
            from_index->id_map.data());
    LOG(DEBUG) << "Ending build_index for file_id=" << file.file_id
        << " with new index_file_id=" << group_file.file_id << std::endl;
    /* std::cout << "raw size=" << from_index->ntotal << "   index size=" << index->ntotal << std::endl; */
    write_index(index, group_file.location.c_str());
    group_file.file_type = meta::GroupFileSchema::INDEX;
    group_file.rows = file.dimension * index->ntotal;

    auto to_remove = file;
    to_remove.file_type = meta::GroupFileSchema::TO_DELETE;

    meta::GroupFilesSchema update_files = {to_remove, group_file};
    _pMeta->update_files(update_files);

    return Status::OK();
}

void DBImpl::background_build_index() {
    std::lock_guard<std::mutex> lock(build_index_mutex_);
    assert(bg_build_index_started_);
    meta::GroupFilesSchema to_index_files;
    _pMeta->files_to_index(to_index_files);
    Status status;
    for (auto& file : to_index_files) {
        status = build_index(file);
        if (!status.ok()) {
            _bg_error = status;
            return;
        }
    }

    bg_build_index_started_ = false;
    bg_build_index_finish_signal_.notify_all();
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
        status = background_merge_files(group_id);
        if (!status.ok()) {
            _bg_error = status;
            return;
        }
    }
}

DBImpl::~DBImpl() {
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _shutting_down.store(true, std::memory_order_release);
        while (_bg_compaction_scheduled) {
            _bg_work_finish_signal.wait(lock);
        }
    }
    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);
        while (bg_build_index_started_) {
            bg_build_index_finish_signal_.wait(lock);
        }
    }
    std::vector<std::string> ids;
    _pMemMgr->serialize(ids);
}

/*
 *  DB
 */

DB::~DB() {}

void DB::Open(const Options& options, DB** dbptr) {
    *dbptr = nullptr;
    *dbptr = new DBImpl(options);
    return;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
