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
#include "db/meta/MetaTypes.h"


namespace zilliz {
namespace milvus {
namespace scheduler {


using engine::meta::TableFileSchemaPtr;

using Id2IndexMap = std::unordered_map<size_t, TableFileSchemaPtr>;
using Id2DistanceMap = std::vector<std::pair<int64_t, double>>;
using ResultSet = std::vector<Id2DistanceMap>;

class SearchJob : public Job {
public:
    SearchJob(JobId id, uint64_t topk, uint64_t nq, uint64_t nprobe, const float *vectors);

public:
    bool
    AddIndexFile(const TableFileSchemaPtr &index_file);

    void
    WaitResult();

    void
    SearchDone(size_t index_id);

    ResultSet &
    GetResult();

public:
    uint64_t
    topk() const {
        return topk_;
    }

    uint64_t
    nq() const {
        return nq_;
    }

    uint64_t
    nprobe() const {
        return nprobe_;
    }
    const float *
    vectors() const {
        return vectors_;
    }

private:
    uint64_t topk_ = 0;
    uint64_t nq_ = 0;
    uint64_t nprobe_ = 0;
    // TODO: smart pointer
    const float *vectors_ = nullptr;

    Id2IndexMap index_files_;
    // TODO: column-base better ?
    ResultSet result_;

    std::mutex mutex_;
    std::condition_variable cv_;
};

using SearchJobPtr = std::shared_ptr<SearchJob>;

}
}
}

