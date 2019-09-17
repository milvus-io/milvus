/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <string>
#include <vector>
#include <list>
#include <queue>
#include <deque>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>

#include "Job.h"
#include "db/meta/Meta.h"


namespace zilliz {
namespace milvus {
namespace scheduler {

class DeleteJob : public Job {
public:
    DeleteJob(JobId id,
              std::string table_id,
              engine::meta::MetaPtr meta_ptr,
              uint64_t num_resource);

public:
    void
    WaitAndDelete();

    void
    ResourceDone();

public:
    std::string
    table_id() const {
        return table_id_;
    }

    engine::meta::MetaPtr
    meta() const {
        return meta_ptr_;
    }

private:
    std::string table_id_;
    engine::meta::MetaPtr meta_ptr_;

    uint64_t num_resource_ = 0;
    uint64_t done_resource = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using DeleteJobPtr = std::shared_ptr<DeleteJob>;

}
}
}

