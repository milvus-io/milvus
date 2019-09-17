/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "utils/Log.h"

#include "SearchJob.h"


namespace zilliz {
namespace milvus {
namespace scheduler {

SearchJob::SearchJob(zilliz::milvus::scheduler::JobId id,
                     uint64_t topk,
                     uint64_t nq,
                     uint64_t nprobe,
                     const float *vectors) : Job(id, JobType::SEARCH) {}

bool
SearchJob::AddIndexFile(const TableFileSchemaPtr &index_file) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (index_file == nullptr || index_files_.find(index_file->id_) != index_files_.end()) {
        return false;
    }

    SERVER_LOG_DEBUG << "SearchJob " << id() << " add index file: " << index_file->id_;

    index_files_[index_file->id_] = index_file;
    return true;
}


void
SearchJob::WaitResult() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return index_files_.empty(); });
    SERVER_LOG_DEBUG << "SearchJob " << id() << " all done";
}

void
SearchJob::SearchDone(size_t index_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    index_files_.erase(index_id);
    cv_.notify_all();
    SERVER_LOG_DEBUG << "SearchJob " << id() << " finish index file: " << index_id;
}

ResultSet &
SearchJob::GetResult() {
    return result_;
}


}
}
}


