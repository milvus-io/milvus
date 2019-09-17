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


namespace zilliz {
namespace milvus {
namespace scheduler {

enum class JobType {
    INVALID,
    SEARCH,
    DELETE,
    BUILD,
};

using JobId = std::uint64_t;

class Job {
public:
    inline JobId
    id() const {
        return id_;
    }

    inline JobType
    type() const {
        return type_;
    }

protected:
    Job(JobId id, JobType type) : id_(id), type_(type) {}

private:
    JobId id_;
    JobType type_;
};

using JobPtr = std::shared_ptr<Job>;

}
}
}

