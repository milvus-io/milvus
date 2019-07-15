/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IScheduleTask.h"
#include "db/scheduler/context/SearchContext.h"
#include "db/ExecutionEngine.h"

namespace zilliz {
namespace milvus {
namespace engine {

class SearchTask : public IScheduleTask {
public:
    SearchTask();

    virtual std::shared_ptr<IScheduleTask> Execute() override;

    static Status ClusterResult(const std::vector<long> &output_ids,
                                const std::vector<float> &output_distence,
                                uint64_t nq,
                                uint64_t topk,
                                SearchContext::ResultSet &result_set);

    static Status MergeResult(SearchContext::Id2DistanceMap &distance_src,
                              SearchContext::Id2DistanceMap &distance_target,
                              uint64_t topk,
                              bool ascending);

    static Status TopkResult(SearchContext::ResultSet &result_src,
                             uint64_t topk,
                             bool ascending,
                             SearchContext::ResultSet &result_target);

public:
    size_t index_id_ = 0;
    int index_type_ = 0; //for metrics
    ExecutionEnginePtr index_engine_;
    std::vector<SearchContextPtr> search_contexts_;
    bool metric_l2 = true;
};

using SearchTaskPtr = std::shared_ptr<SearchTask>;


}
}
}