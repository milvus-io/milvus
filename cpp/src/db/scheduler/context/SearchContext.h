/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IScheduleContext.h"
#include "db/meta/MetaTypes.h"

#include <unordered_map>
#include <vector>
#include <memory>
#include <condition_variable>

namespace zilliz {
namespace milvus {
namespace engine {

using TableFileSchemaPtr = std::shared_ptr<meta::TableFileSchema>;

class SearchContext : public IScheduleContext {
public:
    SearchContext(uint64_t topk, uint64_t nq, uint64_t nprobe, const float* vectors);

    bool AddIndexFile(TableFileSchemaPtr& index_file);

    uint64_t topk() const { return topk_; }
    uint64_t nq() const  { return nq_; }
    uint64_t nprobe() const { return nprobe_; }
    const float* vectors() const { return vectors_; }

    using Id2IndexMap = std::unordered_map<size_t, TableFileSchemaPtr>;
    const Id2IndexMap& GetIndexMap() const { return map_index_files_; }

    using Id2DistanceMap = std::vector<std::pair<int64_t, double>>;
    using ResultSet = std::vector<Id2DistanceMap>;
    const ResultSet& GetResult() const { return result_; }
    ResultSet& GetResult() { return result_; }

    std::string Identity() const { return identity_; }

    void IndexSearchDone(size_t index_id);
    void WaitResult();

    void AccumLoadCost(double span) { time_cost_load_ += span; }
    void AccumSearchCost(double span) { time_cost_search_ += span; }
    void AccumReduceCost(double span) { time_cost_reduce_ += span; }

    double LoadCost() const { return time_cost_load_; }
    double SearchCost() const { return time_cost_search_; }
    double ReduceCost() const { return time_cost_reduce_; }

private:
    uint64_t topk_ = 0;
    uint64_t nq_ = 0;
    uint64_t nprobe_ = 10;
    const float* vectors_ = nullptr;

    Id2IndexMap map_index_files_;
    ResultSet result_;

    std::mutex mtx_;
    std::condition_variable done_cond_;

    std::string identity_; //for debug

    double time_cost_load_ = 0.0; //time cost for load all index files, unit: us
    double time_cost_search_ = 0.0; //time cost for entire search, unit: us
    double time_cost_reduce_ = 0.0; //time cost for entire reduce, unit: us
};

using SearchContextPtr = std::shared_ptr<SearchContext>;



}
}
}
