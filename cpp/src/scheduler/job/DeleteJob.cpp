/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DeleteJob.h"


namespace zilliz {
namespace milvus {
namespace scheduler {

DeleteJob::DeleteJob(JobId id,
                     std::string table_id,
                     engine::meta::MetaPtr meta_ptr,
                     uint64_t num_resource)
    : Job(id, JobType::DELETE),
      table_id_(std::move(table_id)),
      meta_ptr_(std::move(meta_ptr)),
      num_resource_(num_resource) {}

void DeleteJob::WaitAndDelete() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_resource == num_resource_; });
    meta_ptr_->DeleteTableFiles(table_id_);
}

void DeleteJob::ResourceDone() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++done_resource;
    }
    cv_.notify_one();
}

}
}
}

