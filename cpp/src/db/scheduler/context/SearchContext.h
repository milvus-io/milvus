/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IScheduleContext.h"
#include "db/MetaTypes.h"

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
    SearchContext(uint64_t topk, uint64_t nq, const float* vectors);

    bool AddIndexFile(TableFileSchemaPtr& index_file);

    uint64_t topk() const { return topk_; }
    uint64_t nq() const  { return nq_; }
    const float* vectors() const { return vectors_; }

    using Id2IndexMap = std::unordered_map<size_t, TableFileSchemaPtr>;
    const Id2IndexMap& GetIndexMap() const { return map_index_files_; }

    using Id2DistanceMap = std::vector<std::pair<int64_t, double>>;
    using ResultSet = std::vector<Id2DistanceMap>;
    const ResultSet& GetResult() const { return result_; }
    ResultSet& GetResult() { return result_; }

    void IndexSearchDone(size_t index_id);
    void WaitResult();

private:
    uint64_t topk_ = 0;
    uint64_t nq_ = 0;
    const float* vectors_ = nullptr;

    Id2IndexMap map_index_files_;
    ResultSet result_;

    std::mutex mtx_;
    std::condition_variable done_cond_;

    std::string identity_; //for debug
};

using SearchContextPtr = std::shared_ptr<SearchContext>;



}
}
}
